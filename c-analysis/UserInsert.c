#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/user.h>
#include <sys/stat.h>

#include "hiredis.h"
#include "ketama.h"
#include "comm.h"

#define USER_AMOUNT 100
#define ITEM_AMOUNT 10000

enum { max_wkr = USER_AMOUNT };

typedef struct _sync {
    volatile int start;
    struct {
        volatile int ready;
        volatile uint64_t ms;
        char* user;
        char* item[300];
        char* score[300];
        int item_amount;
        long len;
    } wkr[max_wkr];
} sync_t;

sync_t *sync_state;

typedef struct redis_srv {
    const char *host;
    unsigned int port;
} redis_srv_t;

static int nprocs = 0;
static int nwkrs ;
static int nsrvs = 0;
static redis_srv_t *srvs;

static bool_t pipeline = TRUE;
static long pmax = 50L;

//static uint64_t interval;
//static uint64_t last_cycle;

static void
waitup(void)
{
    uint64_t avg, tot, max;
    int i;

    tot = 0;
    max = 0;
    for (i = 0; i < nwkrs; i++) {
        while (!sync_state->wkr[i].ms)
            nop_pause();

        tot += sync_state->wkr[i].ms;
        if (sync_state->wkr[i].ms > max)
            max = sync_state->wkr[i].ms;
    }
    avg = (tot / nwkrs);
    
    printf("avg: %2lu.%03lu max: %2lu.%03lu\n", ms2s(avg), ms2s(max));
}


typedef struct redis_connect {
    redisContext *context;
    long nreqs; /* pipeline */
} redis_connect_t;

static void
emit_single(ketama *k, redis_connect_t *conns, char *user, char* item, char* score, int len)
{
    redisReply *reply;
    int idx = ketama_get_server_ordinal(k, user, len);

    reply = redisCommand(conns[idx].context, "ZADD %s %s %s", user, score, item, len);
    assert(reply != NULL && reply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(reply);
}

static void
emit_pipeline(ketama *k, redis_connect_t *conns, char *user, char* item, char* score, int len)
{
    int idx = ketama_get_server_ordinal(k, user, len);
    redis_connect_t *c = &conns[idx];
    
    assert(redisAppendCommand(c->context, "ZADD %s %s %s", user, score, item, len) == REDIS_OK);
    c->nreqs++;

    if (c->nreqs >= pmax) {
        redisReply *reply;
        while (c->nreqs) {
            assert(redisGetReply(c->context, (void*)&reply) == REDIS_OK);
            assert(reply != NULL && reply->type == REDIS_REPLY_INTEGER);
            freeReplyObject(reply);
            c->nreqs --;
        }
    }
}

static void
emit(ketama *k, redis_connect_t *conns, char *user, char* item, char* score, int len)
{
    if (pipeline) {
        emit_pipeline(k, conns, user, item, score, len);
    } else {
        emit_single(k, conns, user, item, score, len);
    }
}


static void *
worker(void *x)
{
	int wkr = (uintptr_t)x;
	uint64_t s;
    ketama *k;
    redis_connect_t *conns;
    char *user = sync_state->wkr[wkr].user;
    char **item = sync_state->wkr[wkr].item;
    char **score = sync_state->wkr[wkr].score;
	long len = sync_state->wkr[wkr].len;
    
	setaffinity(wkr, nprocs);

    conns = malloc(sizeof(redis_connect_t)*nsrvs);
    k = ketama_new();
    for (int i = 0; i < nsrvs; i++) {
        conns[i].context = redisConnect(srvs[i].host, srvs[i].port);
        if (conns[i].context->err)
            die("connection error: %s", conns[i].context->errstr);

        ketama_add_server(k, srvs[i].host, srvs[i].port, 100);
        printf("connect with server.%d %s:%d (W:%d)\n", 
                i, srvs[i].host, srvs[i].port, 100);
        conns[i].nreqs = 0L;
    }
    ketama_create_continuum(k);
    
	sync_state->wkr[wkr].ready = 1;
	if (wkr)
		while (!sync_state->start)
			nop_pause();
	else
		sync_state->start = 1;
	
	s = mstime();
	
	if(wkr == 19){printf("%s", user);}
	
	for(int i = 0;i<sync_state->wkr[wkr].item_amount;i++)
		emit(k, conns, user, item[i], score[i], len);
	
    for (int i = 0; i < nsrvs; i++) {
        redis_connect_t *c = &conns[i];
        if (pipeline) {
            redisReply *reply;
            while (c->nreqs) {
                assert(redisGetReply(c->context, (void*)&reply) == REDIS_OK);
                assert(reply != NULL && reply->type == REDIS_REPLY_INTEGER);
                freeReplyObject(reply);
                c->nreqs --;
            }
        }
        redisFree(c->context);
    }
    free(conns);
	ketama_free(k);
    sync_state->wkr[wkr].ms = mstime() - s;
    return 0;
}

static void get_data_from_redis(){
	
	redisContext* context;
	redisReply* users;
	redisReply* items;
	redisReply* reply;

	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("redis connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	
	nwkrs = users->elements;
	
	//get the data from redis
	for(int i=0;i < users->elements;i++){

		sync_state->wkr[i].user = users->element[i]->str;

		items = redisCommand(context, "zrange %s 0 300", users->element[i]->str);
		
		sync_state->wkr[i].item_amount = items->elements;

		for(int j = 0;j < items->elements;j++){

			sync_state->wkr[i].item[j] = items->element[j]->str;

			reply = redisCommand(context, "zscore %s %s", users->element[i]->str, items->element[j]->str); 
			sync_state->wkr[i].score[j] = reply->str;
			sync_state->wkr[i].len = sizeof(sync_state->wkr[i].user);

		}
	}
	
}

static void
load_srvs(const char *fn)
{
    FILE *f;
    int port, cnt = 0;
    char *host;

    f = fopen(fn, "r");
    if (!f)
        edie("fopen(%s) failed", fn);
    
    printf("loading configuration file %s\n", fn);

    srvs = (redis_srv_t *)malloc(sizeof(redis_srv_t) * nsrvs);
    while (!feof(f)) {
        host = malloc(sizeof(char) * 16);
        fscanf(f, "%s %d\n", host, &port);
        printf("redis srv.%d %s:%d\n", cnt, host, port);
        
        srvs[cnt].host = (const char *)host;
        srvs[cnt].port = port;
        
        cnt++;
        if (cnt >= nsrvs)
            break;
    }
    if (cnt < nsrvs)
        nsrvs = cnt;
    
    printf("configure %d redis srvs\n", nsrvs);
    fclose(f);
}


int main(int argc, char **argv)
{
    char *srvfn;
    int i;
    
    //infn = argv[1];
    srvfn = "srvs.list";
    
    //printf("enter the amount of servers you want to shard:");
    //scanf("\n%d",&nsrvs);
    nsrvs = 4;
    
    if (nsrvs <=0)
        die("load srvs failed");
    
    if ((nprocs = sysconf(_SC_NPROCESSORS_ONLN)) < 0)
        edie("sysconf failed");
    printf("machine with %d procs\n", nprocs);

    if (nwkrs <= 0)
        nwkrs = nprocs;
    
    if (pmax <= 0)
        pmax = 1L; /* no pipeline */
    
    setaffinity(0, nprocs);
    load_srvs(srvfn);
    
    sync_state = (void *) mmap(0, sizeof(*sync_state), 
                            PROT_READ | PROT_WRITE, 
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
                            
    if (sync_state == MAP_FAILED)
        edie("mmap sync_state failed");
    memset(sync_state, 0, sizeof(*sync_state));

	while(1){

      get_data_from_redis();
    
      printf("start user insert\n");
	  for (i = 0; i <= nwkrs; i++) {
		
        pthread_t th;
		if (pthread_create(&th, NULL, worker, (void *)(intptr_t)i) < 0)
			edie("pthread_create failed");
		
		while (!sync_state->wkr[i].ready)
			nop_pause();
	  }
	
	  printf("Wait for 20 seconds to refresh......");
	  sleep(20);
	
	}

	//worker((void *)(intptr_t)0);
	//waitup();
	return 0;
}
