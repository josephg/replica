#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct DatabaseHandle DatabaseHandle;

typedef void (*CCallback)(void*);

void hello_world(void);

struct DatabaseHandle *database_new(void);

void database_free(struct DatabaseHandle *this_);

void database_start(struct DatabaseHandle *this_, void *signal_data, CCallback signal_callback);

uint64_t database_num_posts(struct DatabaseHandle *this_);
