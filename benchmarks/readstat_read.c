/*
 * Read one SAS7BDAT file once with the ReadStat C library and print one JSON line,
 * for benchmarks/run.py.
 *
 * Usage: readstat_read <path>
 *
 * ReadStat delivers values through callbacks. This program stores every value
 * the way a columnar reader would: numbers as doubles in one array per column,
 * strings copied into one array per column. The timed region is the parse,
 * storing included. The checksum is computed afterwards and matches the other
 * readers: numeric columns give their sum and missing count, string columns the
 * total UTF-8 length after removing trailing spaces.
 *
 * Build: cc -O2 -o readstat_read readstat_read.c -lreadstat
 */
#include <math.h>
#include <readstat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    char name[256];
    int is_string;
    double *nums;
    char **strs;
} column;

typedef struct {
    int ncols;
    long nrows;
    long capacity;
    long row_hint;
    column *cols;
} table;

static void grow(table *t, long rows) {
    if (rows <= t->capacity) return;
    long cap = t->capacity ? t->capacity : 1024;
    while (cap < rows) cap *= 2;
    for (int i = 0; i < t->ncols; i++) {
        column *c = &t->cols[i];
        if (c->is_string) {
            c->strs = realloc(c->strs, (size_t)cap * sizeof(char *));
            memset(c->strs + t->capacity, 0, (size_t)(cap - t->capacity) * sizeof(char *));
        } else {
            c->nums = realloc(c->nums, (size_t)cap * sizeof(double));
        }
    }
    t->capacity = cap;
}

static int on_metadata(readstat_metadata_t *m, void *ctx) {
    table *t = ctx;
    t->ncols = readstat_get_var_count(m);
    t->cols = calloc((size_t)t->ncols, sizeof(column));
    t->row_hint = readstat_get_row_count(m);
    return READSTAT_HANDLER_OK;
}

static int on_variable(int index, readstat_variable_t *v, const char *labels, void *ctx) {
    (void)labels;
    table *t = ctx;
    column *c = &t->cols[index];
    snprintf(c->name, sizeof c->name, "%s", readstat_variable_get_name(v));
    c->is_string = readstat_variable_get_type_class(v) == READSTAT_TYPE_CLASS_STRING;
    /* Column types are known once the last variable arrives; allocate then. */
    if (index == t->ncols - 1 && t->row_hint > 0) grow(t, t->row_hint);
    return READSTAT_HANDLER_OK;
}

static int on_value(int obs, readstat_variable_t *v, readstat_value_t value, void *ctx) {
    table *t = ctx;
    int i = readstat_variable_get_index(v);
    if (obs >= t->capacity) grow(t, (long)obs + 1);
    if (obs + 1 > t->nrows) t->nrows = obs + 1;
    column *c = &t->cols[i];
    if (c->is_string) {
        const char *s = readstat_string_value(value);
        c->strs[obs] = (s && !readstat_value_is_system_missing(value)) ? strdup(s) : NULL;
    } else {
        c->nums[obs] = readstat_value_is_missing(value, v) ? NAN : readstat_double_value(value);
    }
    return READSTAT_HANDLER_OK;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <path>\n", argv[0]);
        return 2;
    }
    table t = {0};
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    readstat_parser_t *p = readstat_parser_init();
    readstat_set_metadata_handler(p, on_metadata);
    readstat_set_variable_handler(p, on_variable);
    readstat_set_value_handler(p, on_value);
    readstat_error_t err = readstat_parse_sas7bdat(p, argv[1], &t);
    readstat_parser_free(p);
    clock_gettime(CLOCK_MONOTONIC, &b);
    if (err != READSTAT_OK) {
        fprintf(stderr, "readstat: %s\n", readstat_error_message(err));
        return 1;
    }
    double seconds = (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9;

    printf("{\"tool\":\"readstat-c\",\"seconds\":%.6f,\"rows\":%ld,\"cols\":%d,\"checksum\":{",
           seconds, t.nrows, t.ncols);
    for (int i = 0; i < t.ncols; i++) {
        column *c = &t.cols[i];
        if (i) putchar(',');
        if (c->is_string) {
            unsigned long long total = 0;
            for (long r = 0; r < t.nrows; r++) {
                const char *s = c->strs[r];
                if (!s) continue;
                size_t n = strlen(s);
                while (n > 0 && s[n - 1] == ' ') n--;
                total += n;
            }
            printf("\"%s\":[\"str\",%llu]", c->name, total);
        } else {
            double sum = 0.0;
            long missing = 0;
            for (long r = 0; r < t.nrows; r++) {
                if (c->nums[r] != c->nums[r]) missing++;
                else sum += c->nums[r];
            }
            printf("\"%s\":[\"num\",%.17g,%ld]", c->name, sum, missing);
        }
    }
    printf("}}\n");
    return 0;
}
