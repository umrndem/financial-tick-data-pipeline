#ifndef COMMON_HPP
#define COMMON_HPP

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

static const int CHUNK_MAGIC = 0x43484E4B;  // CHNK
static const int SHM_MAGIC = 0x53484D31;    // SHM1

static const int CHUNK_TYPE_DATA = 1;
static const int CHUNK_TYPE_EOF = 2;
static const int CHUNK_TYPE_POISON = 3;

static const int EXIT_OK = 0;
static const int EXIT_BAD_ARGS = 10;
static const int EXIT_IPC_FAIL = 20;
static const int EXIT_CHILD_DIED = 30;
static const int EXIT_IO_FAIL = 40;
static const int EXIT_SIGINT = 130;
static const int EXIT_SIGTERM = 143;

static const int MAX_SYMBOL_LEN = 32;
static const int MAX_AGG_ENTRIES = 1024;

struct ChunkHeader {
    int magic;
    int type;
    int chunk_id;
    int source_file_id;
    int byte_count;
};

struct AggEntry {
    char symbol[MAX_SYMBOL_LEN];
    double pv_sum;
    double high;
    double low;
    long long total_volume;
    long long record_count;
};

struct ShmLayout {
    int magic;
    int entry_count;
    long long total_records;
    AggEntry entries[MAX_AGG_ENTRIES];
};

inline int write_all(int fd, const void *buf, int count) {
    const char *p = static_cast<const char *>(buf);
    int left = count;
    while (left > 0) {
        int n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += n;
        left -= n;
    }
    return count;
}

inline int read_all(int fd, void *buf, int count) {
    char *p = static_cast<char *>(buf);
    int left = count;
    while (left > 0) {
        int n = read(fd, p, left);
        if (n == 0) {
            return count - left;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += n;
        left -= n;
    }
    return count;
}

inline void log_line(const char *component, const string &msg) {
    cerr << '[' << component << " pid=" << getpid() << " ppid=" << getppid() << "] " << msg << '\n';
}

#endif
