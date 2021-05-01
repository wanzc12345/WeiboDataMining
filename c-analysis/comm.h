#ifndef _COMM_H
#define _COMM_H

#include <sched.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

typedef enum { FALSE, TRUE } bool_t;

#define ms2s(_ms) ((_ms)/1000), ((_ms)%1000)
#define us2ms(_us) ((_us)/1000), ((_us)%1000)

extern long long ustime(void);
extern long long mstime(void);

extern void die(const char* errstr, ...);
extern void edie(const char* errstr, ...);

static inline void setaffinity(int c, int nprocs)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(c%nprocs, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0)
        edie("setaffinity, sched_setaffinity failed");
}

static inline void nop_pause(void)
{
    __asm __volatile("pause");
}

static inline void rep_nop(void)
{
    __asm __volatile("rep; nop" ::: "memory");
}

static inline void cpu_relax(void)
{
    rep_nop();
}

static inline uint64_t read_tsc(void)
{
    uint32_t a, d;
    __asm __volatile("rdtsc" : "=a" (a), "=d" (d));
    return ((uint64_t) a) | (((uint64_t) d) << 32);
}


#endif
