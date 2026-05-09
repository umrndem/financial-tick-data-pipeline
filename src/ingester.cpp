#include "common/common.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <vector>

using namespace std;

static volatile sig_atomic_t g_stop = 0;
static volatile sig_atomic_t g_dump = 0;

static long long g_files_processed = 0;
static long long g_chunks_sent = 0;
static long long g_bytes_sent = 0;

static void signal_handler(int signo) {
    if (signo == SIGTERM || signo == SIGINT) {
        g_stop = signo;
    } else if (signo == SIGUSR1) {
        g_dump = 1;
    }
}

static bool has_csv_ext(const string &name) {
    if (name.size() < 4) {
        return false;
    }
    return name.substr(name.size() - 4) == ".csv";
}

static int write_chunk(int fd, int type, int chunk_id, int file_id, const string &payload) {
    ChunkHeader h;
    h.magic = CHUNK_MAGIC;
    h.type = type;
    h.chunk_id = chunk_id;
    h.source_file_id = file_id;
    h.byte_count = payload.size();

    if (write_all(fd, &h, sizeof(h)) < 0) {
        return -1;
    }
    if (!payload.empty() && write_all(fd, payload.data(), payload.size()) < 0) {
        return -1;
    }
    return 0;
}

static int flush_data_chunk(int fifo_fd, int &chunk_id, int file_id, string &payload) {
    if (payload.empty()) {
        return 0;
    }
    if (write_chunk(fifo_fd, CHUNK_TYPE_DATA, chunk_id++, file_id, payload) < 0) {
        return -1;
    }
    ++g_chunks_sent;
    g_bytes_sent += payload.size();
    payload.clear();
    return 0;
}

static int process_file_posix(int fifo_fd, const string &path, int file_id, int &chunk_id,
                              int max_rows_per_chunk) {
    int in_fd = open(path.c_str(), O_RDONLY);
    if (in_fd < 0) {
        perror("open input file");
        return -1;
    }

    string payload;
    payload.reserve(64 * 1024);
    string carry;
    carry.reserve(8192);
    int rows_in_chunk = 0;

    char buf[8192];
    while (!g_stop) {
            int n = read(in_fd, buf, sizeof(buf));
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("read input file");
            close(in_fd);
            return -1;
        }

            carry.append(buf, n);
            int start = 0;
        while (!g_stop) {
                int end = static_cast<int>(carry.find('\n', start));
                if (end < 0) {
                break;
            }

                string line = carry.substr(start, end - start);
            start = end + 1;
            if (line.empty()) {
                continue;
            }

            payload += line;
            payload.push_back('\n');
            ++rows_in_chunk;

            if (rows_in_chunk >= max_rows_per_chunk || payload.size() >= 64 * 1024) {
                if (flush_data_chunk(fifo_fd, chunk_id, file_id, payload) < 0) {
                    perror("write chunk");
                    close(in_fd);
                    return -1;
                }
                rows_in_chunk = 0;
            }

            if (g_dump) {
                g_dump = 0;
                fprintf(stderr,
                    "[ingester pid=%d ppid=%d] files=%lld chunks=%lld bytes=%lld\n",
                    static_cast<int>(getpid()), static_cast<int>(getppid()),
                    g_files_processed, g_chunks_sent, g_bytes_sent);
            }
        }
            carry.erase(0, start);
    }

    if (!g_stop && !carry.empty()) {
        payload += carry;
        payload.push_back('\n');
        ++rows_in_chunk;
    }

    if (flush_data_chunk(fifo_fd, chunk_id, file_id, payload) < 0) {
        perror("write chunk");
        close(in_fd);
        return -1;
    }

    close(in_fd);
    return 0;
}

int main(int argc, char *argv[]) {
    string input_dir;
    string fifo_path;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-i" && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (a == "--fifo" && i + 1 < argc) {
            fifo_path = argv[++i];
        } else {
            fprintf(stderr, "Usage: %s -i <input_dir> --fifo <fifo_path>\n", argv[0]);
            return EXIT_BAD_ARGS;
        }
    }

    if (input_dir.empty() || fifo_path.empty()) {
        return EXIT_BAD_ARGS;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGUSR1, &sa, nullptr);

    DIR *dir = opendir(input_dir.c_str());
    if (!dir) {
        perror("opendir");
        return EXIT_IO_FAIL;
    }

    vector<string> files;
    while (true) {
        errno = 0;
        dirent *ent = readdir(dir);
        if (!ent) {
            if (errno != 0) {
                perror("readdir");
                closedir(dir);
                return EXIT_IO_FAIL;
            }
            break;
        }
        string name = ent->d_name;
        if (has_csv_ext(name)) {
            files.push_back(input_dir + "/" + name);
        }
    }
    closedir(dir);

    sort(files.begin(), files.end());
    if (files.empty()) {
        log_line("ingester", "no CSV files found");
        return EXIT_IO_FAIL;
    }

    log_line("ingester", "opening FIFO for writing");
    int fifo_fd = open(fifo_path.c_str(), O_WRONLY);
    if (fifo_fd < 0) {
        perror("open fifo");
        return EXIT_IO_FAIL;
    }

    int chunk_id = 1;
    const int max_rows_per_chunk = 1000;

    int file_count = files.size();
    for (int file_idx = 0; file_idx < file_count && !g_stop; ++file_idx) {
            if (process_file_posix(fifo_fd, files[file_idx], file_idx, chunk_id,
                               max_rows_per_chunk) < 0) {
            close(fifo_fd);
            return EXIT_IO_FAIL;
        }

        ++g_files_processed;
    }

    if (write_chunk(fifo_fd, CHUNK_TYPE_EOF, chunk_id++, 0, "") < 0) {
        perror("write eof");
        close(fifo_fd);
        return EXIT_IO_FAIL;
    }

    close(fifo_fd);
    log_line("ingester", "finished sending chunks");

    if (g_stop == SIGINT) {
        return EXIT_SIGINT;
    }
    if (g_stop == SIGTERM) {
        return EXIT_SIGTERM;
    }
    return EXIT_OK;
}
