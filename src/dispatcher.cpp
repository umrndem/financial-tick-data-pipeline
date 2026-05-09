#include "common/common.hpp"

#include <fcntl.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>

#include <cstdlib>
#include <string>
#include <vector>

using namespace std;

struct ChildInfo {
    string name;
    pid_t pid;
    time_t start_time;
    time_t end_time;
    int raw_status;
    bool exited;
};

static volatile sig_atomic_t g_shutdown_signal = 0;
static volatile sig_atomic_t g_sigchld_seen = 0;
static volatile sig_atomic_t g_sigusr1_seen = 0;

static void signal_handler(int signo) {
    if (signo == SIGINT || signo == SIGTERM) {
        g_shutdown_signal = signo;
    } else if (signo == SIGCHLD) {
        g_sigchld_seen = 1;
    } else if (signo == SIGUSR1) {
        g_sigusr1_seen = 1;
    }
}

static int spawn_child(const string &name, const vector<string> &args, const string &log_path,
                       vector<ChildInfo> &children) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        int log_fd = open(log_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (log_fd < 0) {
            perror("open log");
            _exit(EXIT_IO_FAIL);
        }
        if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) {
            perror("dup2");
            close(log_fd);
            _exit(EXIT_IO_FAIL);
        }
        close(log_fd);

        vector<char *> argv;
        for (const string &arg : args) {
            argv.push_back(const_cast<char *>(arg.c_str()));
        }
        argv.push_back(nullptr);

        execvp(argv[0], argv.data());
        perror("execvp");
        _exit(EXIT_IO_FAIL);
    }

    ChildInfo info;
    info.name = name;
    info.pid = pid;
    info.start_time = time(nullptr);
    info.end_time = 0;
    info.raw_status = 0;
    info.exited = false;
    children.push_back(info);
    return 0;
}

int main(int argc, char *argv[]) {
    string input_dir;
    string output_dir;
    string fifo_path;
    string shm_name;
    string sem_name;
    int threads = 0;
    int queue_size = 0;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-i" && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (a == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (a == "-n" && i + 1 < argc) {
            threads = atoi(argv[++i]);
        } else if (a == "-q" && i + 1 < argc) {
            queue_size = atoi(argv[++i]);
        } else if (a == "--fifo" && i + 1 < argc) {
            fifo_path = argv[++i];
        } else if (a == "--shm" && i + 1 < argc) {
            shm_name = argv[++i];
        } else if (a == "--sem" && i + 1 < argc) {
            sem_name = argv[++i];
        } else {
            fprintf(stderr, "Usage: %s -i <input> -o <output> -n <threads> -q <queue> --fifo <path> --shm <name> --sem <name>\n",
                    argv[0]);
            return EXIT_BAD_ARGS;
        }
    }

    if (input_dir.empty() || output_dir.empty() || fifo_path.empty() || shm_name.empty() ||
        sem_name.empty() || threads <= 0 || queue_size <= 0) {
        fprintf(stderr, "dispatcher: bad arguments\n");
        return EXIT_BAD_ARGS;
    }

    mkdir("logs", 0755);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGINT, &sa, nullptr) < 0 || sigaction(SIGTERM, &sa, nullptr) < 0 ||
        sigaction(SIGCHLD, &sa, nullptr) < 0 || sigaction(SIGUSR1, &sa, nullptr) < 0) {
        perror("sigaction");
        return EXIT_IO_FAIL;
    }

    unlink(fifo_path.c_str());
    if (mkfifo(fifo_path.c_str(), 0666) < 0) {
        perror("mkfifo");
        return EXIT_IPC_FAIL;
    }

    int shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("shm_open");
        unlink(fifo_path.c_str());
        return EXIT_IPC_FAIL;
    }
    if (ftruncate(shm_fd, sizeof(ShmLayout)) < 0) {
        perror("ftruncate");
        close(shm_fd);
        shm_unlink(shm_name.c_str());
        unlink(fifo_path.c_str());
        return EXIT_IPC_FAIL;
    }

    void *shm_ptr = mmap(nullptr, sizeof(ShmLayout), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        shm_unlink(shm_name.c_str());
        unlink(fifo_path.c_str());
        return EXIT_IPC_FAIL;
    }
    memset(shm_ptr, 0, sizeof(ShmLayout));
    ShmLayout *layout = static_cast<ShmLayout *>(shm_ptr);
    layout->magic = SHM_MAGIC;

    sem_unlink(sem_name.c_str());
    sem_t *named_sem = sem_open(sem_name.c_str(), O_CREAT, 0666, 0);
    if (named_sem == SEM_FAILED) {
        perror("sem_open");
        munmap(shm_ptr, sizeof(ShmLayout));
        close(shm_fd);
        shm_unlink(shm_name.c_str());
        unlink(fifo_path.c_str());
        return EXIT_IPC_FAIL;
    }
    sem_close(named_sem);

    vector<ChildInfo> children;

    vector<string> ingester_args;
    ingester_args.push_back("./ingester");
    ingester_args.push_back("-i");
    ingester_args.push_back(input_dir);
    ingester_args.push_back("--fifo");
    ingester_args.push_back(fifo_path);

    vector<string> processor_args;
    processor_args.push_back("./processor");
    processor_args.push_back("--fifo");
    processor_args.push_back(fifo_path);
    processor_args.push_back("--shm");
    processor_args.push_back(shm_name);
    processor_args.push_back("--sem");
    processor_args.push_back(sem_name);
    processor_args.push_back("-n");
    processor_args.push_back(to_string(threads));
    processor_args.push_back("-q");
    processor_args.push_back(to_string(queue_size));

    vector<string> reporter_args;
    reporter_args.push_back("./reporter");
    reporter_args.push_back("-o");
    reporter_args.push_back(output_dir);
    reporter_args.push_back("--shm");
    reporter_args.push_back(shm_name);
    reporter_args.push_back("--sem");
    reporter_args.push_back(sem_name);

    if (spawn_child("processor", processor_args, "logs/processor.log", children) < 0 ||
        spawn_child("ingester", ingester_args, "logs/ingester.log", children) < 0 ||
        spawn_child("reporter", reporter_args, "logs/reporter.log", children) < 0) {
        for (const ChildInfo &child : children) {
            kill(child.pid, SIGTERM);
        }
        munmap(shm_ptr, sizeof(ShmLayout));
        close(shm_fd);
        shm_unlink(shm_name.c_str());
        unlink(fifo_path.c_str());
        sem_unlink(sem_name.c_str());
        return EXIT_IO_FAIL;
    }

    log_line("dispatcher", "children spawned");

    sigset_t mask;
    sigemptyset(&mask);

    auto all_done = [&children]() {
        for (const ChildInfo &child : children) {
            if (!child.exited) {
                return false;
            }
        }
        return true;
    };

    bool terminate_sent = false;

    while (!all_done()) {
        if (g_sigusr1_seen) {
            g_sigusr1_seen = 0;
            log_line("dispatcher", "SIGUSR1 received (reporter ready signal)");
        }

        if (g_shutdown_signal && !terminate_sent) {
            terminate_sent = true;
            for (const ChildInfo &child : children) {
                if (!child.exited) {
                    kill(child.pid, SIGTERM);
                }
            }
        }

        if (g_sigchld_seen || g_shutdown_signal) {
            g_sigchld_seen = 0;
            while (true) {
                int st = 0;
                pid_t p = waitpid(-1, &st, WNOHANG);
                if (p <= 0) {
                    break;
                }
                for (ChildInfo &child : children) {
                    if (child.pid == p) {
                        child.raw_status = st;
                        child.end_time = time(nullptr);
                        child.exited = true;
                    }
                }
            }
        }

        if (!all_done()) {
            sigsuspend(&mask);
        }
    }

    munmap(shm_ptr, sizeof(ShmLayout));
    close(shm_fd);
    shm_unlink(shm_name.c_str());
    unlink(fifo_path.c_str());
    sem_unlink(sem_name.c_str());

    int final_code = EXIT_OK;
    if (g_shutdown_signal == SIGINT) {
        final_code = EXIT_SIGINT;
    } else if (g_shutdown_signal == SIGTERM) {
        final_code = EXIT_SIGTERM;
    }

    for (ChildInfo &c : children) {
        int code = -1;
        if (WIFEXITED(c.raw_status)) {
            code = WEXITSTATUS(c.raw_status);
        }
        if (WIFSIGNALED(c.raw_status)) {
            code = 128 + WTERMSIG(c.raw_status);
            final_code = EXIT_CHILD_DIED;
        }

        double runtime = difftime(c.end_time, c.start_time);
        fprintf(stderr, "[dispatcher pid=%d ppid=%d] child=%s pid=%d exit=%d runtime=%.0f sec\n",
                static_cast<int>(getpid()), static_cast<int>(getppid()), c.name.c_str(),
                static_cast<int>(c.pid), code, runtime);
    }

    return final_code;
}
