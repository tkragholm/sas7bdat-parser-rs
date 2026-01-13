#if !defined(_WIN32)
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif

#include <readstat.h>
#include <stdio.h>
#include <stdlib.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <time.h>
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC CLOCK_REALTIME
#endif
#endif

typedef struct {
    long row_count;
    long non_null_count;
    int var_count;
} bench_context_t;

static int metadata_handler(readstat_metadata_t *metadata, void *ctx) {
    bench_context_t *bench = (bench_context_t *)ctx;
    bench->var_count = readstat_get_var_count(metadata);
    return READSTAT_HANDLER_OK;
}

static int value_handler(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) {
    bench_context_t *bench = (bench_context_t *)ctx;

    if (!readstat_value_is_missing(value, variable)) {
        bench->non_null_count++;
    }

    if (readstat_variable_get_index(variable) == bench->var_count - 1) {
        bench->row_count = obs_index + 1;
    }

    return READSTAT_HANDLER_OK;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file.sas7bdat>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *path = argv[1];
    readstat_parser_t *parser = readstat_parser_init();
    bench_context_t context = {0};

    readstat_set_metadata_handler(parser, &metadata_handler);
    readstat_set_value_handler(parser, &value_handler);

#if defined(_WIN32)
    LARGE_INTEGER freq;
    LARGE_INTEGER start, end;
    if (!QueryPerformanceFrequency(&freq)) {
        fprintf(stderr, "High-resolution performance counter not available on this system.\n");
        readstat_parser_free(parser);
        return EXIT_FAILURE;
    }
    QueryPerformanceCounter(&start);
#else
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
#endif

    readstat_error_t err = readstat_parse_sas7bdat(parser, path, &context);

#if defined(_WIN32)
    QueryPerformanceCounter(&end);
#else
    clock_gettime(CLOCK_MONOTONIC, &end);
#endif

    readstat_parser_free(parser);

    if (err != READSTAT_OK) {
        fprintf(stderr, "ReadStat error %d processing %s\n", err, path);
        return EXIT_FAILURE;
    }

    double elapsed_ms;
#if defined(_WIN32)
    elapsed_ms = (end.QuadPart - start.QuadPart) * 1000.0 / (double)freq.QuadPart;
#else
    elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 +
                 (end.tv_nsec - start.tv_nsec) / 1e6;
#endif

    printf("File            : %s\n", path);
    printf("Rows processed  : %ld\n", context.row_count);
    printf("Columns         : %d\n", context.var_count);
    printf("Non-null cells  : %ld\n", context.non_null_count);
    printf("Elapsed (ms)    : %.2f\n", elapsed_ms);

    return EXIT_SUCCESS;
}
