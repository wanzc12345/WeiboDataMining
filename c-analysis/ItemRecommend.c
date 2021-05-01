/*
 * BigDataProcessing.c
 * 
 * Copyright 2012 Jensen Wan <jensen@jensen-Lenovo-IdeaPad-Y550>
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 * 
 * 
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include "hiredis.h"
#include <time.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/user.h>
#include <sys/stat.h>

#include "hiredis.h"
#include "ketama.h"
#include "comm.h"

#define USER_AMOUNT 100
#define ITEM_AMOUNT 5000
#define THREAD_AMOUNT 100

typedef struct redis_srv {
    const char *host;
    unsigned int port;
    redisReply *keys;
} redis_srv_t;

static int nsrvs = 0;
static redis_srv_t *srvs;

typedef struct _wkr {
    redis_srv_t *srv;
    long off;
    long cnt;

    volatile uint64_t ms;
} wkr_t;

static int nwkrs = 0;
wkr_t *wkrs;

static int nprocs = 0;

static void load_srvs(const char *fn)
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

#if 0
static void waitup(void)
{
    uint64_t avg, tot, max;
    int i;

    tot = 0;
    max = 0;
    for (i = 0; i < nwkrs; i++) {
        while (!sync_state->wkr[i].cycle)
            nop_pause();

        tot += sync_state->wkr[i].cycle;
        if (sync_state->wkr[i].cycle > max)
            max = sync_state->wkr[i].cycle;
    }
    avg = (tot / nwkrs);
    
    
    printf("avg: %2lu.%03lu max: %2lu.%03lu\n", 
            c2s(avg), c2ms(avg), c2s(max), c2ms(max));
}
#endif

void main_work();

void show_menu();

void manual();

void initialization();

void preparation();

void preparation_multithread();

void preparation_sharded_multithread();

void trash();

int generate_rating_table(int**);

int generate_rating_table_multi_thread(int**);

int generate_rating_table_sharded_multi_thread(int**);

int calculate_simularity(int**, double**);

int generate_recommend_items(char*, int**, double**);

int generate_all_recommend(int**, double**);

int generate_all_recommend_multi_thread(int**, double**);

void * worker(void* arg);

void * rating_worker(void* arg);

void * rating_sharding_worker(void* arg);

//global scope variables
int** rating_table;				//user's rates on items
double** simularity_table;		//items' simularity score
int order;						//multithread output order
int ending;						//thread end signal
int parameter[USER_AMOUNT];		//parameter
char** all_users;  				//all users
char** all_items;				//all items
int real_users_amount;			//real users amount
int real_item_amount;			//real item amount
double total_time;				//total run time
double ostart, ofinish;		//run time

int main(int argc, char **argv)
{
	
	//main loop
	while(1){
	
		//count time elapse
		clock_t start, finish;
	
		start = clock();
	
		main_work();
		
		while(1){
			
			if(ending == 1){
				
				finish = clock();
				printf("Total time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
				
				printf("Waiting for 30 seconds to refresh...\n");
				fflush(stdout);
				
				break;
			}
		}
	
		sleep(15);
		
	}
	
	//release
	trash();
	return 0;
	
}

void show_menu(){
	
	printf("\n*************J's Recommendation*************\n\n1. auto-simple\n2. auto-multithread\n3. auto-sharding-multithread\n4. manual\n0. exit\n\nYou choose:");
	
}

void main_work(){
	
	char option;
	
	//show menu
	show_menu();
	
	//release the buffer
	setbuf(stdin, NULL);
	
	//get the option
	scanf("\n%c", &option);
	
	switch(option){
		case '1':
			
			//preparation work
			preparation();
			
			//automatically output
			printf("recommendation result:\n");
			generate_all_recommend(rating_table, simularity_table);
			break;
			
		case '2':
			
			//preparation work
			preparation_multithread();
			
			//automatically output in multithread mode
			printf("recommendation result:\n");
			generate_all_recommend_multi_thread(rating_table, simularity_table);
			break;
		
		case '3':
		
			//preparation work
			preparation_sharded_multithread();
			
			//sharding mode
			generate_all_recommend_multi_thread(rating_table, simularity_table);
			break;
			
		case '4':
		
			//preparation work
			preparation();
		
			//manual mode
			manual();
			break;
		
		default:
			exit(0);
			break;
			
	}
	
}

void manual(){
	
	//main loop
	char choice = 'y';
	while(choice == 'y'){
		
		char uid[10];
		
		printf("Please enter the user's name whom you want to recommend item for:");
		scanf("%s", uid);
		printf("\nGood!\n\nNow waiting...\n");
		
		generate_recommend_items(uid, rating_table, simularity_table);
			
		printf("Continue?(y/n):");
		choice=getchar();
		scanf("%c",&choice);
	}

}

void trash(){
	
	free(rating_table);
	free(simularity_table);
	free(all_users);
	free(all_items);
	
}

void preparation(){
		
	//initial
	initialization();
	
	//generate rating table
	printf("generating rates table...\n");
	generate_rating_table(rating_table);
	printf("complete!\n\n");
	//getchar();
	
	//calculate simularity
	printf("calculating simularity...\n");
	calculate_simularity(rating_table, simularity_table);
	printf("complete!\n\n");
	//getchar();
	
}

void preparation_multithread(){
	
	//initial
	initialization();
	
	//generate rating table
	printf("generating rates table...\n");
	generate_rating_table_multi_thread(rating_table);
	printf("complete!\n\n");
	//getchar();
	
	//calculate simularity
	printf("calculating simularity...\n");
	calculate_simularity(rating_table, simularity_table);
	printf("complete!\n\n");
	//getchar();
	
}

void preparation_sharded_multithread(){
	
	//initial
	initialization();
	
	//generate rating table
	printf("generating rates table...\n");
	generate_rating_table_sharded_multi_thread(rating_table);
	printf("complete!\n\n");
	//getchar();
	
	//calculate simularity
	printf("calculating simularity...\n");
	calculate_simularity(rating_table, simularity_table);
	printf("complete!\n\n");
	//getchar();
	
}

void initialization(){
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	//initialize rating table and simularity table
	printf("\ninitializing...\n");
	
	//construct connect

	redisContext* context;
	redisReply* users;
	redisReply* items;

	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);
	
	real_users_amount = users->elements;
	real_item_amount = items->elements;
	
	all_users = (char**)malloc(sizeof(char*)*USER_AMOUNT);
	
	for(int i=0;i<USER_AMOUNT;i++){
		all_users[i] = (char*)malloc(sizeof(char)*50);
	}
	
	for(int i=0;i<users->elements;i++){
		all_users[i] = users->element[i]->str;
	}
	
	all_items = (char**)malloc(sizeof(char*)*ITEM_AMOUNT);
	
	for(int i=0;i<ITEM_AMOUNT;i++){
		all_items[i] = (char*)malloc(sizeof(char)*200);
	}
	
	for(int i=0;i<items->elements;i++){
		all_items[i] = items->element[i]->str;
	}
	
	rating_table = (int**)malloc(sizeof(int*)*USER_AMOUNT);

	for(int i=0;i<USER_AMOUNT;i++){
		rating_table[i] = (int*)malloc(sizeof(int)*ITEM_AMOUNT);
	}
	
	simularity_table = (double**)malloc(sizeof(double*)*ITEM_AMOUNT);
	for(int i=0;i<ITEM_AMOUNT;i++){
		simularity_table[i] = (double*)malloc(sizeof(double)*ITEM_AMOUNT);
	}
	
	order = 0;
	
	ending = 0;
	
	printf("complete!\n\n");
	
	finish = clock();
	printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
	
}

int generate_rating_table(int** rating_table){

	//count time elapse
	clock_t start, finish;
	
	start = clock();

	//construct connect
	int i, j;
	redisContext* context;
	redisReply* users;
	redisReply* items;
	redisReply* reply;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);

	//main loop
	for(i=0;i<users->elements;i++){
		for(j=0;j<items->elements;j++){
			
			reply = redisCommand(context, "zscore %s %s", users->element[i]->str, items->element[j]->str);
			if(reply->str!=NULL){
				sscanf(reply->str, "%d", &rating_table[i][j]);
			}else
				rating_table[i][j] = 0;
			
			//printf("%d", rating_table[i][j]);
		}

	}
	
	//close and return
	freeReplyObject(users);
	freeReplyObject(items);
	freeReplyObject(reply);
	
	finish = clock();
	printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
	
	return 1;
}

int generate_rating_table_multi_thread(int** rating_table){
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	//construct connect

	redisContext* context;
	redisReply* users;
	redisReply* items;	
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);

	pthread_t threads[THREAD_AMOUNT];
	//int parameter[USER_AMOUNT];

	//main loop
	for(int i=0;i<users->elements;i++){
		
		//sleep(1);
		parameter[i] = i;
		int err = pthread_create(&threads[i], NULL, rating_worker, (void *)&parameter[i]);
		if(err != 0){
			printf("can't create the thread: %s\n", strerror(err));
			return 0;
		}

	}
	
	int check = 0;
	//check if finished
	while(1){
		check = 1;
		for(int i = 0; i < users->elements; i++){
			//printf("%d\n",rating_table[i][items->elements]);
			if(rating_table[i][items->elements]!=-1)
				check = 0;
		}
		
		if(check ==1){
				
				finish = clock();
				printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
				break;
				
		}
		//printf("check:%d\n", check);
	}
	
	//close and return
	freeReplyObject(users);
	freeReplyObject(items);

	return 1;
	
}

int generate_rating_table_sharded_multi_thread(int** rating_table){
	
	//printf("Enter the amount of servers:");
    //scanf("\n%d", &nsrvs);
    nsrvs = 4;
    setbuf(stdin, NULL);
    nwkrs = nsrvs;
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	//construct connect

	redisContext* context;
	redisReply* users;
	redisReply* items;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);


	pthread_attr_t attr;
	pthread_t *tid;
    char *srvfn;
    uint64_t s;
    
    srvfn = "srvs.list";
    
    if (nsrvs <= 0)
        die("load srvs failed");
    
    if ((nprocs = sysconf(_SC_NPROCESSORS_ONLN)) < 0)
        edie("sysconf failed");
    printf("machine with %d procs and %d workers\n", nprocs, nwkrs);

    if (nwkrs <= 0)
        nwkrs = nsrvs;
    
    setaffinity(0, nprocs);
    load_srvs(srvfn);

    wkrs = (wkr_t *)malloc(sizeof(wkr_t)*nwkrs);
    memset(wkrs, 0, sizeof(sizeof(wkr_t)*nwkrs));

	for(int i=0;i<nwkrs;i++){
		wkrs[i].srv = &srvs[i];
	}

    s = mstime();

    tid = (pthread_t *)malloc(sizeof(pthread_t)*nwkrs);
    pthread_attr_init(&attr);
	pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
	for (int i = 0; i < nwkrs; i++) {		
        if (pthread_create(&tid[i], NULL, rating_sharding_worker, (void *)(intptr_t)i) < 0)
			edie("pthread_create failed");
	}
    
	for (int i = 0; i < nwkrs; i++) {
		int ret_val;
		assert(pthread_join(tid[i], (void **) (void *) &ret_val) == 0);
		assert(ret_val == 0);
	}

    s = mstime() - s;
    
    int check = 0;
	//check if finished
	while(1){
		check = 1;
		for(int i = 0; i < users->elements; i++){
			
			//printf("i:%d %d\n", i, rating_table[i][items->elements]);
			//getchar();
			
			if(rating_table[i][items->elements]!=-1)
				check = 0;
		}
		
		if(check ==1){
				
				finish = clock();
				printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
				break;
				
		}
		//printf("check:%d\n", check);
	}

    for (int i = 0; i < nsrvs; i++)
        freeReplyObject(srvs[i].keys);
	return 0;
	
	//close and return
	freeReplyObject(users);
	freeReplyObject(items);

	return 1;
	
}

void* rating_sharding_worker(void* x){
	
	int w = (uintptr_t)x;
    wkr_t *wkr = &wkrs[w];
    redisContext *con;

    long off = wkr->off, cnt = wkr->cnt;

    uint64_t s;
    
	setaffinity(w, nprocs);
	printf("wkr.%d start, off:%ld cnt:%ld\n", w, off, cnt);
    
	s = mstime();
	
	//printf("host:%s port:%u\n", wkr->srv->host, wkr->srv->port);
				
    con = redisConnect(wkr->srv->host, wkr->srv->port);
    
    //fetch keys
    wkr->srv->keys = redisCommand(con, "KEYS *");
    
    for (int i = 0; i < wkr->srv->keys->elements; i++) {
        
        char* user;
        
        redisReply *items;
		
        redisReply *key = wkr->srv->keys->element[i];
        assert(key != NULL && key->type == REDIS_REPLY_STRING);
        user = key->str;
        
        //printf("fuck you bitch!");
			//	fflush(stdout);
			
		items = redisCommand(con,"ZRANGE %s 0 %d", user, ITEM_AMOUNT);
        //assert(items != NULL && items->type == REDIS_REPLY_STRING);
        
        /*insert rating to table*/
        for(int j = 0;j < real_users_amount;j++){
			
			if(!strcmp(all_users[j], user)){
				
				redisReply *reply;
				
				//printf("fuck you bitch!");
				//fflush(stdout);
				
				int k;
				for(k=0;k<real_item_amount;k++){
					
					for(int s = 0; s < items->elements;s++){
						
						if(!strcmp(all_items[k], items->element[s]->str)){
							reply = redisCommand(con, "zscore %s %s", user, items->element[s]->str);
							if(reply->str!=NULL){
								sscanf(reply->str, "%d", &rating_table[j][k]);
							}else
							rating_table[j][k] = 0;
						}
						
					}
			
					//printf("%d", rating_table[i][j]);
				}
				
				//printf("j;%d k:%d\n",j,k);
				
				//ending signal
				rating_table[j][k] = -1;

				break;
			}
		}
        
        freeReplyObject(items);
	}
    redisFree(con);
    
    wkr->ms = mstime() - s;
    return 0;
	
}

void* rating_worker(void* arg){
	
	//construct connect

	redisContext* context;
	redisReply* users;
	redisReply* items;
	redisReply* reply;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);
	
	int i = *(int*)arg;
	if(i>=users->elements||i<0){
		printf("******************%d\n",i);
		fflush(stdout);
		return (void*)0;
	}
	
	int j;
	
	for(j=0;j<items->elements;j++){
			
			reply = redisCommand(context, "zscore %s %s", users->element[i]->str, items->element[j]->str);
			if(reply->str!=NULL){
				sscanf(reply->str, "%d", &rating_table[i][j]);
			}else
				rating_table[i][j] = 0;
			
			//printf("%d", rating_table[i][j]);
	}
	
	//ending signal
	rating_table[i][j] = -1;
	
	return (void*)0;
}

int calculate_simularity(int** rating_table, double** simularity_table){
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	redisContext* context;
	redisReply* users;
	redisReply* items;

	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	//initialization
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);
	
	int i,j;
	double sum1=0,sum2=0,sum3=0;
	
	for(i=0;i<items->elements;i++){
		for(j=i;j<items->elements;j++)
		{
			sum1 = sum2 = sum3 = 0;
			int k;
			for(k=0;k<users->elements;k++){
				sum1 += rating_table[k][i] * rating_table[k][j];
				sum2 += rating_table[k][i] * rating_table[k][i];
				sum3 += rating_table[k][j] * rating_table[k][j];
			}
			
			simularity_table[i][j] = simularity_table[j][i] = sum1/(sqrt(sum2)*sqrt(sum3));
			//printf("%d %d %lf\t", i, j, simularity_table[i][j]);
		}
	
	}
		
	freeReplyObject(users);
	freeReplyObject(items);
	
	finish = clock();
	printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
	
	return 1;
}

int generate_recommend_items(char* user_name, int** rating_table, double** simularity_table){
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	int i,j;
	
	redisContext* context;
	redisReply* reply;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}

	if(redisCommand(context, "zscore users %s", user_name)!=NULL)
		reply = redisCommand(context, "zscore users %s", user_name);
	else 
		return 0;
		
	sscanf(reply->str,"%d",&i);

	double sum = 0, sum2 = 0, highscore = 0;
	int most_index;
	
	for(j=0;j<ITEM_AMOUNT;j++){

		int k;
		for(k=0;k<ITEM_AMOUNT;k++){
			if(rating_table[i][j]==0){
				sum += rating_table[i][k]*simularity_table[j][k];
				sum2 += simularity_table[j][k];
			}
		//	printf("j:%d k:%d\n",j,k);
		}

		if(highscore<(sum/sum2)){
			highscore = sum/sum2;
			most_index = j;
		}
	}

	reply = redisCommand(context, "zrange %s %d %d", "items", most_index, most_index);

	printf("You might like this: %s\n", reply->element[0]->str);
	
	//free the space
	freeReplyObject(reply);
	
	finish = clock();
	printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
	
	return 1;
}

int generate_all_recommend(int** rating_table, double** simularity_table){
	
	//count time elapse
	clock_t start, finish;
	
	start = clock();
	
	int i,j;
	
	redisContext* context;
	redisReply* reply;
	redisReply* users;
	redisReply* items;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}
	
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);

	printf("(No.\tid\t\tname\n\test_score\titem_name)\n\n");

	double highscore;

	for(i=0;i<users->elements;i++){

	double sum, sum2;
	highscore = 0;
	int most_index = -1;
	
	//printf("*************i:\t%d*************\n", i);
	//getchar();
	
	for(j=0;j<items->elements;j++){

		if(rating_table[i][j]==0){
			
			//printf("j:%d",j);
			
			sum = 0;
			sum2 = 0;
		
			int k;
			for(k=0;k<items->elements;k++){
				if(rating_table[i][k]!=0){
					sum += rating_table[i][k]*simularity_table[j][k];
					//printf("i:%d j:%d k:%d sum:%f\n", i, j, k, sum);
					sum2 += simularity_table[j][k];
				}
			}
			//printf("j:%d k:%d\n",j,k);
			
			//printf("\tscore\t%lf\n", sum/sum2);
			
			if(sum2!=0){
				if(highscore<(sum/sum2)){
					highscore = sum/sum2;
					most_index = j;
				}
			}
		
		}
	}
	
	reply = redisCommand(context, "zrange names %d %d", i, i);
	
	printf("%d)\t%s\t%s\n", i, users->element[i]->str,reply->element[0]->str);

	reply = redisCommand(context, "zrange items %d %d", most_index, most_index);
	
	if(highscore!=0)
		printf("\t%f\t%s\n\n", highscore, reply->element[0]->str);
	else
		printf("\t%f\t'fail'\n\n",highscore);
	
	}
	
	order = users->elements;
	
	ending = 1;
	
	//free the space
	freeReplyObject(reply);
	freeReplyObject(items);
	freeReplyObject(users);
	
	finish = clock();
	printf("Time consume: %f s.\n\n", (double)(finish-start)/CLOCKS_PER_SEC);
	
	return 1;
}

int generate_all_recommend_multi_thread(int** rating_table, double** simularity_table){
	
	//count time elapse
	ostart = clock();
	
	redisContext* context;

	redisReply* users;

	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}
	
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);

	printf("(No.\tid\t\tname\n\test_score\titem_name)\n\n");
	
	pthread_t threads[THREAD_AMOUNT];
	//int parameter[USER_AMOUNT];
	
	//printf("%d", (int)users->elements);
	//getchar();
	
	for(int i=0;i<users->elements;i++){

		parameter[i] = i;
		int err = pthread_create(&threads[i], NULL, worker, (void *)&parameter[i]);
		if(err != 0){
			printf("can't create the thread: %s\n", strerror(err));
			return 0;
		}
	
	}
	
	//free the space
	freeReplyObject(users);

	//finish = clock();
	//printf("Time consume: %f s.\n", (double)((finish-start)/CLOCKS_PER_SEC));

	return 1;
}

void *worker(void * arg){
	
	int i=*(int*)arg;
	
	char result1[100], result2[100];
	
	redisContext* context;
	redisReply* reply;
	redisReply* users;
	redisReply* items;
	struct timeval timeout = {1, 500000};
	context = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if(context->err){
		printf("connection error: %s\n", context->errstr);
		exit(1);
	}
	
	users = redisCommand(context, "zrange %s 0 %d", "users", USER_AMOUNT);
	items = redisCommand(context, "zrange items 0 %d", ITEM_AMOUNT);
	
	/*if(i==users->elements||i<0){
		//printf("%d", i);
		return (void *)0;
	}*/
	
	double sum, sum2;
	double highscore = 0;
	int most_index = -1;
	
	//printf("*************i:\t%d*************\n", i);
	//getchar();
	
	for(int j=0;j<items->elements;j++){
		if(rating_table[i][j]==0){
			
			//printf("j:%d",j);
			
			sum = 0;
			sum2 = 0;
		
			int k;
			for(k=0;k<items->elements;k++){
				if(rating_table[i][k]!=0){
					sum += rating_table[i][k]*simularity_table[j][k];
					//printf("i:%d j:%d k:%d sum:%f\n", i, j, k, sum);
					sum2 += simularity_table[j][k];
				}
			}
			//printf("j:%d k:%d\n",j,k);
			
			//printf("\tscore\t%lf\n", sum/sum2);
			
			if(sum2!=0){
				if(highscore<(sum/sum2)){
					highscore = sum/sum2;
					most_index = j;
				}
			}
		
		}
	}

	reply = redisCommand(context, "zrange names %d %d", i, i);

	sprintf(result1, "%d)\t%s\t%s\n", i, users->element[i]->str,reply->element[0]->str);

	reply = redisCommand(context, "zrange items %d %d", most_index, most_index);

	if(highscore!=0)
		sprintf(result2, "\t%f\t%s\n\n", highscore, reply->element[0]->str);
	else
		sprintf(result2, "\t%f\t'fail'\n\n",highscore);
	
	//output in order
	while(1){
		if(order==i){
			printf("%s%s", result1, result2);
			fflush(stdout);
			order++;
			if(order == users->elements){
				
				ofinish = clock();
				printf("Time consume: %f s.\n\n", (double)(ofinish-ostart)/CLOCKS_PER_SEC);
				ending = 1;
			}
			break;
		}
	}
	
	return (void *)0;
	
}
