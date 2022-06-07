#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include "mapreduce.h"
#include "hashmap.h"

struct kv {
    char* key;
    char* value;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

// struct kv_list kvl;
// size_t kvl_counter;

struct kv_list* partitions;
size_t* counters;

pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t* locks;
Mapper mymapper;
Reducer myreducer;
Partitioner mypartitioner;
int num_files;      // number of files for reading
int *file_pool;     // 0 is not assigned, 1 if assigned
int num_complete;
void *child_mapper(void *args);  // mapper
void init_file_pool(int *file_pool);  // initialize the each element of file_pool to be zero
int find_not_assigned(int *file_pool);  // return a index for a file which is not assigned
int num_partitions;

void init_kv_list(struct kv_list* kvl, size_t size) {
    kvl->elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl->num_elements = 0;
    kvl->size = size;
}

void add_to_list(struct kv_list* kvl, struct kv* elt) {
    if (kvl->num_elements == kvl->size) {
	kvl->size *= 2;
	kvl->elements = realloc(kvl->elements, kvl->size * sizeof(struct kv*));
    }
    kvl->elements[kvl->num_elements++] = elt;
}

char* get_func(char* key, int partition_number) {
    struct kv_list* kvl = &partitions[partition_number];

    if (counters[partition_number] == kvl->num_elements) {
	    return NULL;
    }
    struct kv *curr_elt = kvl->elements[counters[partition_number]];
    if (!strcmp(curr_elt->key, key)) {
	    counters[partition_number]++;
	    return curr_elt->value;
    }
    return NULL;
}

int cmp(const void* a, const void* b) {
    char* str1 = (*(struct kv **)a)->key;
    char* str2 = (*(struct kv **)b)->key;
    return strcmp(str1, str2);
}

void MR_Emit(char* key, char* value)
{
    struct kv *elt = (struct kv*) malloc(sizeof(struct kv));
    if (elt == NULL) {
	    printf("Malloc error! %s\n", strerror(errno));
	    exit(1);
    }
    elt->key = strdup(key);
    elt->value = strdup(value);
    int p_number = (*mypartitioner)(key, num_partitions);
    
    // need lock here
    pthread_mutex_lock(&locks[p_number]);
    add_to_list(&partitions[p_number],elt);
    pthread_mutex_unlock(&locks[p_number]);

    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void *child_mapper(void *args){
    while(1){
        // get an file for reading
        pthread_mutex_lock(&m1);
        int index = find_not_assigned(file_pool);
        if(index == -1){ // all files are assigned, exit
            pthread_mutex_unlock(&m1);
            return NULL;
        }
        file_pool[index] = 1;  // mark the file as assigned
        pthread_mutex_unlock(&m1);

        // run map()
        (*mymapper)(((char **)args)[index + 1]);
    }
}

void *child_reducer(void *args){
    int p_number = *(int *)args;
    pthread_mutex_unlock(&m2);

    while (counters[p_number] < partitions[p_number].num_elements) {
	    (*myreducer)((partitions[p_number].elements[counters[p_number]])->key, get_func, p_number);
    }

    return NULL;
}

void init_file_pool(int *file_pool){
    for(int i = 0; i < num_files; i++){
        file_pool[i] = 0;
    }
    return;
}

int find_not_assigned(int *file_pool){
    int i = 0;
    for(i = 0; i < num_files; i++){
        if(file_pool[i] == 0){
            return i;
        }
    }
    return -1;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers, Partitioner partition)
{
    // init_kv_list(10);
    num_partitions = num_reducers;
    mymapper = map;
    myreducer = reduce;
    mypartitioner = partition;
    num_files = argc - 1;
    partitions = (struct kv_list*)malloc(num_partitions * sizeof(struct kv_list));
    counters = (size_t *)malloc(num_partitions * sizeof(size_t));
    pthread_t* mapper_threads = (pthread_t*)malloc(num_mappers * sizeof(pthread_t));
    pthread_t* reducer_threads = (pthread_t*)malloc(num_reducers * sizeof(pthread_t));
    file_pool = (int *)malloc(num_files * sizeof(int));
    init_file_pool(file_pool);
    

    // initialize partitions
    for(int i = 0; i < num_partitions; i++){
        init_kv_list(&partitions[i], 10);
    }

    // initialize counters
    for(int i = 0; i < num_partitions; i++){
        counters[i] = 0;
    }

    // initialize locks for paritions
    locks = (pthread_mutex_t*)malloc(num_partitions * sizeof(pthread_mutex_t));
    for(int i = 0; i < num_partitions; i++){
        pthread_mutex_init(&locks[i], NULL);
    }

    // create mapper threads
    int count = 0;
    while(count != num_mappers){
        if(pthread_create(&mapper_threads[count], NULL, child_mapper, argv) == 0){
            count++;
            continue;
        }else{
            continue;
        }
    }

    // wait for all mapper threads complete
    count = 0;
    while(count != num_mappers){
        if(pthread_join(mapper_threads[count], NULL) == 0){
            count++;
            continue;
        }else{
            continue;
        }
    }

    // now mappers are done, sort paritions
    for(int i = 0; i < num_partitions; i++){
        qsort(partitions[i].elements, partitions[i].num_elements, sizeof(struct kv*), cmp);
    }
    
    // create reducer threads
    count = 0;
    while(count != num_reducers){
        pthread_mutex_lock(&m2);
        if(pthread_create(&reducer_threads[count], NULL, child_reducer, &count) == 0){

            pthread_mutex_lock(&m2);
            count++;
            pthread_mutex_unlock(&m2);
            continue;
        }else{
            pthread_mutex_unlock(&m2);
            continue;
        }
    }

    // wait for all reducer threads
    count = 0;
    while(count != num_reducers){
        if(pthread_join(reducer_threads[count], NULL) == 0){
            count++;
            continue;
        }else{
            continue;
        }
    }

    // done mapping and reducing entirely! Free everything inside MR
    for(int i = 0; i < num_partitions; i++){
        for(int j = 0; j < partitions[i].num_elements; j++){
            free(partitions[i].elements[j]);
        }
        free(partitions[i].elements);
    }
    free(partitions);
    free(counters);
    free(locks);
    free(mapper_threads);
    free(reducer_threads);
    free(file_pool);


    return;
}
