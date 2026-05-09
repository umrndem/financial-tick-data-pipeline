#include "common/common.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

struct QueueItem {
    int type;
    string payload;
};

struct AggStats {
    double pv_sum;
    double high;
    double low;
    long long volume;
    long long count;
};

static int g_fifo_fd = -1;
static int g_threads = 0;
static int g_queue_size = 0;
static string g_shm_name;
static string g_sem_name;

static deque<QueueItem> g_queue;
static sem_t g_sem_empty;
static sem_t g_sem_full;
static pthread_mutex_t g_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_agg_mutex = PTHREAD_MUTEX_INITIALIZER;

static unordered_map<string, AggStats> g_agg;
static long long g_total_records = 0;

static volatile sig_atomic_t g_stop = 0;

static void signal_handler(int signo) {
    if (signo == SIGTERM || signo == SIGINT) {
        g_stop = signo;
    }
}

static bool parse_tick_line(const string &line, string &symbol, double &price, long long &volume) {
    vector<string> fields;
    fields.reserve(8);

    string token;
    stringstream ss(line);
    while (getline(ss, token, ',')) {
        fields.push_back(token);
    }

    if (fields.size() < 3) {
        return false;
    }

    symbol = fields[0];
    if (symbol.empty()) {
        return false;
    }

    char *endp_price = nullptr;
    char *endp_vol = nullptr;

    price = strtod(fields[1].c_str(), &endp_price);
    long long vol = strtoll(fields[2].c_str(), &endp_vol, 10);

    if (endp_price == fields[1].c_str() || *endp_price != '\0') {
        return false;
    }
    if (endp_vol == fields[2].c_str() || *endp_vol != '\0') {
        return false;
    }

    volume = vol;
    return true;
}

static void aggregate_payload(const string &payload) {
    string line;
    stringstream ss(payload);
    while (getline(ss, line)) {
        if (line.empty()) {
            continue;
        }

        string symbol;
        double price = 0.0;
        long long volume = 0;
        if (!parse_tick_line(line, symbol, price, volume)) {
            continue;
        }

        pthread_mutex_lock(&g_agg_mutex);
        AggStats &s = g_agg[symbol];
        if (s.count == 0) {
            s.high = price;
            s.low = price;
            s.pv_sum = 0.0;
            s.volume = 0;
            s.count = 0;
        }
        s.pv_sum += price * static_cast<double>(volume);
        s.volume += volume;
        s.count += 1;
        s.high = max(s.high, price);
        s.low = min(s.low, price);
        ++g_total_records;
        pthread_mutex_unlock(&g_agg_mutex);
    }
}

static void queue_push(const QueueItem &item) {
    if (sem_wait(&g_sem_empty) < 0) {
        return;
    }
    pthread_mutex_lock(&g_queue_mutex);
    g_queue.push_back(item);
    pthread_mutex_unlock(&g_queue_mutex);
    sem_post(&g_sem_full);
}

static QueueItem queue_pop() {
    QueueItem item;
    item.type = CHUNK_TYPE_POISON;

    if (sem_wait(&g_sem_full) < 0) {
        return item;
    }
    pthread_mutex_lock(&g_queue_mutex);
    if (!g_queue.empty()) {
        item = g_queue.front();
        g_queue.pop_front();
    }
    pthread_mutex_unlock(&g_queue_mutex);
    sem_post(&g_sem_empty);
    return item;
}

static void *reader_thread_main(void *) {
    while (!g_stop) {
        ChunkHeader h;
        int got = read_all(g_fifo_fd, &h, sizeof(h));
        if (got == 0) {
            break;
        }
        if (got < 0 || got != sizeof(h)) {
            break;
        }
        if (h.magic != CHUNK_MAGIC) {
            break;
        }

        string payload;
        if (h.byte_count > 0) {
            payload.resize(h.byte_count);
            int b = read_all(g_fifo_fd, &payload[0], h.byte_count);
            if (b < 0 || b != h.byte_count) {
                break;
            }
        }

        if (h.type == CHUNK_TYPE_EOF) {
            for (int i = 0; i < g_threads; ++i) {
                QueueItem p;
                p.type = CHUNK_TYPE_POISON;
                queue_push(p);
            }
            return nullptr;
        }

        if (h.type == CHUNK_TYPE_DATA) {
            QueueItem it;
            it.type = CHUNK_TYPE_DATA;
            it.payload = payload;
            queue_push(it);
        }
    }

    for (int i = 0; i < g_threads; ++i) {
        QueueItem p;
        p.type = CHUNK_TYPE_POISON;
        queue_push(p);
    }
    return nullptr;
}

static void *worker_thread_main(void *) {
    while (true) {
        QueueItem it = queue_pop();
        if (it.type == CHUNK_TYPE_POISON) {
            break;
        }
        aggregate_payload(it.payload);
    }
    return nullptr;
}

int main(int argc, char *argv[]) {
    string fifo_path;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "--fifo" && i + 1 < argc) {
            fifo_path = argv[++i];
        } else if (a == "--shm" && i + 1 < argc) {
            g_shm_name = argv[++i];
        } else if (a == "--sem" && i + 1 < argc) {
            g_sem_name = argv[++i];
        } else if (a == "-n" && i + 1 < argc) {
            g_threads = atoi(argv[++i]);
        } else if (a == "-q" && i + 1 < argc) {
            g_queue_size = atoi(argv[++i]);
        } else {
            fprintf(stderr,
                    "Usage: %s --fifo <fifo> --shm <shm_name> --sem <sem_name> -n <threads> -q <queue>\n",
                    argv[0]);
            return EXIT_BAD_ARGS;
        }
    }

    if (fifo_path.empty() || g_shm_name.empty() || g_sem_name.empty() || g_threads <= 0 ||
        g_queue_size <= 0) {
        return EXIT_BAD_ARGS;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    g_fifo_fd = open(fifo_path.c_str(), O_RDONLY);
    if (g_fifo_fd < 0) {
        perror("open fifo");
        return EXIT_IO_FAIL;
    }

    if (sem_init(&g_sem_empty, 0, g_queue_size) < 0 ||
        sem_init(&g_sem_full, 0, 0) < 0) {
        perror("sem_init");
        close(g_fifo_fd);
        return EXIT_IPC_FAIL;
    }

    pthread_t reader_thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 1024 * 1024);

    if (pthread_create(&reader_thread, &attr, reader_thread_main, nullptr) != 0) {
        perror("pthread_create reader");
        pthread_attr_destroy(&attr);
        close(g_fifo_fd);
        sem_destroy(&g_sem_empty);
        sem_destroy(&g_sem_full);
        return EXIT_IO_FAIL;
    }

    vector<pthread_t> workers(g_threads);
    for (int i = 0; i < g_threads; ++i) {
        if (pthread_create(&workers[i], &attr, worker_thread_main, nullptr) != 0) {
            perror("pthread_create worker");
            g_stop = SIGTERM;
            QueueItem p;
            p.type = CHUNK_TYPE_POISON;
            queue_push(p);
            workers[i] = 0;
        }
    }

    pthread_attr_destroy(&attr);

    pthread_join(reader_thread, nullptr);
    for (int i = 0; i < g_threads; ++i) {
        if (workers[i] != 0) {
            pthread_join(workers[i], nullptr);
        }
    }

    int shm_fd = shm_open(g_shm_name.c_str(), O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("shm_open");
        close(g_fifo_fd);
        sem_destroy(&g_sem_empty);
        sem_destroy(&g_sem_full);
        return EXIT_IPC_FAIL;
    }

    void *ptr = mmap(nullptr, sizeof(ShmLayout), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        close(g_fifo_fd);
        sem_destroy(&g_sem_empty);
        sem_destroy(&g_sem_full);
        return EXIT_IPC_FAIL;
    }

    ShmLayout *layout = static_cast<ShmLayout *>(ptr);
    memset(layout, 0, sizeof(*layout));
    layout->magic = SHM_MAGIC;
    layout->total_records = g_total_records;

    int idx = 0;
    for (unordered_map<string, AggStats>::const_iterator it = g_agg.begin();
         it != g_agg.end() && idx < MAX_AGG_ENTRIES; ++it) {
        strncpy(layout->entries[idx].symbol, it->first.c_str(), MAX_SYMBOL_LEN - 1);
        layout->entries[idx].symbol[MAX_SYMBOL_LEN - 1] = '\0';
        layout->entries[idx].pv_sum = it->second.pv_sum;
        layout->entries[idx].high = it->second.high;
        layout->entries[idx].low = it->second.low;
        layout->entries[idx].total_volume = it->second.volume;
        layout->entries[idx].record_count = it->second.count;
        ++idx;
    }
    layout->entry_count = idx;

    sem_t *done_sem = sem_open(g_sem_name.c_str(), 0);
    if (done_sem == SEM_FAILED) {
        perror("sem_open named");
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        close(g_fifo_fd);
        sem_destroy(&g_sem_empty);
        sem_destroy(&g_sem_full);
        return EXIT_IPC_FAIL;
    }

    sem_post(done_sem);
    sem_close(done_sem);

    munmap(ptr, sizeof(ShmLayout));
    close(shm_fd);
    close(g_fifo_fd);
    sem_destroy(&g_sem_empty);
    sem_destroy(&g_sem_full);
    pthread_mutex_destroy(&g_queue_mutex);
    pthread_mutex_destroy(&g_agg_mutex);

    log_line("processor", "aggregation complete and shared memory published");

    if (g_stop == SIGINT) {
        return EXIT_SIGINT;
    }
    if (g_stop == SIGTERM) {
        return EXIT_SIGTERM;
    }
    return EXIT_OK;
}
