#include "common/common.hpp"

#include <fcntl.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#include <string>

using namespace std;

int main(int argc, char *argv[]) {
    string output_dir;
    string shm_name;
    string sem_name;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (a == "--shm" && i + 1 < argc) {
            shm_name = argv[++i];
        } else if (a == "--sem" && i + 1 < argc) {
            sem_name = argv[++i];
        } else {
            fprintf(stderr, "Usage: %s -o <output_dir> --shm <shm_name> --sem <sem_name>\n", argv[0]);
            return EXIT_BAD_ARGS;
        }
    }

    if (output_dir.empty() || shm_name.empty() || sem_name.empty()) {
        return EXIT_BAD_ARGS;
    }

    sem_t *done_sem = sem_open(sem_name.c_str(), 0);
    if (done_sem == SEM_FAILED) {
        perror("sem_open");
        return EXIT_IPC_FAIL;
    }

    if (sem_wait(done_sem) < 0) {
        perror("sem_wait");
        sem_close(done_sem);
        return EXIT_IPC_FAIL;
    }
    sem_close(done_sem);

    int shm_fd = shm_open(shm_name.c_str(), O_RDONLY, 0666);
    if (shm_fd < 0) {
        perror("shm_open");
        return EXIT_IPC_FAIL;
    }

    void *ptr = mmap(nullptr, sizeof(ShmLayout), PROT_READ, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        return EXIT_IPC_FAIL;
    }

    const ShmLayout *layout = static_cast<const ShmLayout *>(ptr);
    if (layout->magic != SHM_MAGIC) {
        fprintf(stderr, "reporter: invalid shared memory magic\n");
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IPC_FAIL;
    }

    string csv_path = output_dir + "/report.csv";
    int csv_fd = open(csv_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (csv_fd < 0) {
        perror("open report.csv");
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }

    if (dprintf(csv_fd, "symbol,vwap,high,low,total_volume,records\n") < 0) {
        perror("write report.csv header");
        close(csv_fd);
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }

    for (int i = 0; i < layout->entry_count; ++i) {
        const AggEntry &e = layout->entries[i];
        double vwap = 0.0;
        if (e.total_volume > 0) {
            vwap = e.pv_sum / static_cast<double>(e.total_volume);
        }
        if (dprintf(csv_fd, "%s,%.2f,%.2f,%.2f,%lld,%lld\n", e.symbol, vwap, e.high, e.low,
                    e.total_volume, e.record_count) < 0) {
            perror("write report.csv row");
            close(csv_fd);
            munmap(ptr, sizeof(ShmLayout));
            close(shm_fd);
            return EXIT_IO_FAIL;
        }
    }
    close(csv_fd);

    string txt_path = output_dir + "/report.txt";

    // Demonstrate dup()/dup2(): save stdout, redirect to report file, print using printf, restore stdout.
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout < 0) {
        perror("dup");
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }

    int out_fd = open(txt_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (out_fd < 0) {
        perror("open report.txt");
        close(saved_stdout);
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }

    if (dup2(out_fd, STDOUT_FILENO) < 0) {
        perror("dup2 report");
        close(out_fd);
        close(saved_stdout);
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }
    close(out_fd);

    printf("Financial Tick Data Report\n");
    printf("Reporter PID: %d Parent PID: %d\n", getpid(), getppid());
    printf("Total Symbols: %d\n", layout->entry_count);
    printf("Total Records: %lld\n\n", layout->total_records);
    printf("%-16s %-12s %-12s %-12s %-12s %-10s\n", "Symbol", "VWAP", "High", "Low", "Volume", "Records");

    for (int i = 0; i < layout->entry_count; ++i) {
        const AggEntry &e = layout->entries[i];
        double vwap = 0.0;
        if (e.total_volume > 0) {
            vwap = e.pv_sum / static_cast<double>(e.total_volume);
        }
        printf("%-16s %-12.2f %-12.2f %-12.2f %-12lld %-10lld\n", e.symbol, vwap, e.high,
               e.low, e.total_volume, e.record_count);
    }
    fflush(stdout);

    if (dup2(saved_stdout, STDOUT_FILENO) < 0) {
        perror("restore stdout");
        close(saved_stdout);
        munmap(ptr, sizeof(ShmLayout));
        close(shm_fd);
        return EXIT_IO_FAIL;
    }
    close(saved_stdout);

    kill(getppid(), SIGUSR1);

    munmap(ptr, sizeof(ShmLayout));
    close(shm_fd);

    log_line("reporter", "report.txt and report.csv generated");
    return EXIT_OK;
}
