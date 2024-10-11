#include "request.h"
#include "server_thread.h"
#include "common.h"

struct server {
	int nr_threads;
	int max_requests;
	int max_cache_size;
	int exiting;
	/* add any other parameters you need */
        struct cache_table* server_cache;
	int *conn_buf;
	pthread_t *threads;
	int request_head;
	int request_tail;
	pthread_mutex_t mutex;
        pthread_mutex_t lock;
	pthread_cond_t prod_cond;
	pthread_cond_t cons_cond;	
};

#define CACHE_TABLE_SIZE 5000

struct cache_entry {
    struct cache_entry* next;
    int in_use;
    struct file_data* requested_data;
};

struct cache_table {
    int size;
    int total_file_size; // size of the total file 
    struct lru_link* lru_list;
    struct cache_entry** cachetable;
};

// lru is stored int he cache, as long as cache is not being destroyed, lru will be available through cache
struct lru_node {
    char* file_name;
    struct lru_node* next;
    struct lru_node* prev;
};

struct lru_link {
    struct lru_node * head;
    struct lru_node * tail;
};
/* static functions */
struct cache_table* cache_init();
struct cache_entry* cache_lookup(char* file_name, struct server *sv); // get the name of file from data get from client and return the file data
struct cache_entry* cache_insert(struct server* sv, struct file_data* file_data);
int cache_evict(struct server* sv, int coming_file_size);
int hash (char* str);
void lru_push (struct lru_node* node, struct lru_link* list);
struct lru_node* lru_free_one (char* file_name, struct lru_link* list);
struct lru_node* lru_free_pointer(struct lru_node* node, struct lru_link* list);
struct lru_node* lru_free_tail(struct lru_link* list); // get the least recently used one and pop it due to reach of max size of the cache
struct lru_node* bring_to_head(char* file_name, struct lru_link* list); // upon on recent access cache hit!
//pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER Add it to the Server Struct and initialize in the server init


// push a new node to the head of the linked list
void lru_push(struct lru_node* node, struct lru_link* list) {
    node->next = list->head;
    node->prev = NULL;
    if (list->tail == NULL) {
        list->tail = node;
    } else {
        list->head->prev = node;
    }
    list->head = node;
    return;
}

struct lru_node* bring_to_head(char* file_name, struct lru_link* list){
    // delete the node from the linked list
    struct lru_node* found_node = lru_free_one(file_name, list);
    // Add it back to the head of the linked list 
    lru_push(found_node, list);
    return found_node;
}

struct lru_node* lru_free_tail(struct lru_link* list) {
    struct lru_node* temp = list->tail;
    if (temp == NULL) {
        return NULL;
    } else {
        if (temp->prev == NULL) { // it is the only node in the list
            list->head = NULL;
        } else {
            temp->prev->next = NULL;
        }
        list->tail = temp->prev;
    }
    return temp;
}

struct lru_node* lru_free_one (char* file_name, struct lru_link* list){
    struct lru_node* temp = list->head;
    while (temp != NULL) {
        if (strcmp(file_name, temp->file_name)) {
            // check if the node is head 
            if (temp->prev == NULL) {
                list->head = temp->next;
            } else {
                temp->prev->next = temp->next;
            }
            if (temp->next == NULL) {
                list->tail = temp->prev;
            } else {
                temp->next->prev = temp->prev;
            }
            return temp;
        }
        temp = temp->next;
    }
    return NULL;
}

/* initialize file data */
static struct file_data *
file_data_init(void)
{
	struct file_data *data;

	data = Malloc(sizeof(struct file_data));
	data->file_name = NULL;
	data->file_buf = NULL;
	data->file_size = 0;
	return data;
}

/* free all file data */
static void
file_data_free(struct file_data *data)
{
	free(data->file_name);
	free(data->file_buf);
	free(data);
}

static void
do_server_request(struct server *sv, int connfd)
{
	int ret;
	struct request *rq;
	struct file_data *data;

	data = file_data_init();

	/* fill data->file_name with name of the file being requested */
	rq = request_init(connfd, data);
	if (!rq) {
		file_data_free(data);
		return;
	}
        if (sv->max_cache_size == 0) {
            ret = request_readfile(rq); 
            if (ret == 0) { /* couldn't read file */
		goto out;    
            }
            request_sendfile(rq);
        } else if (data->file_size > (0.5 * sv->max_cache_size)) {
            ret = request_readfile(rq); 
            if (ret == 0) { /* couldn't read file */
		goto out;    
            }
            request_sendfile(rq);
        } else {
            //printf("cache_size > 0\n");
            pthread_mutex_lock(&sv->lock);
            struct cache_entry* cache_data = (struct cache_entry*)malloc(sizeof(struct cache_entry));
            cache_data = cache_lookup(data->file_name, sv);
            if (cache_data != NULL) {
                // cache hit!
                //printf("cache hit!\n");
                cache_data->in_use++;
                data->file_buf = strdup(cache_data->requested_data->file_buf);
                data->file_size = cache_data->requested_data->file_size;
                //data->file_name = strdup(cache_data->requested_data->file_name);
                request_set_data(rq, data);
                // reserve for updating LRU
                bring_to_head(cache_data->requested_data->file_name, sv->server_cache->lru_list);
                //pthread_mutex_unlock(&sv->lock);
            } else {
                // cache miss! Go to disk and update and insert cache 
                // And update LRU
                //printf("cache miss!\n");
                pthread_mutex_unlock(&sv->lock);
                ret = request_readfile(rq); // rq & data both work data = rq -> data; data -> file_name to access the file name requested
                if (ret == 0) { /* couldn't read file */
                    goto out;    
                }
                pthread_mutex_lock(&sv->lock);
                cache_data = cache_insert(sv, data);
                //pthread_mutex_unlock(&sv->lock);
                //request_sendfile(rq);
                if (cache_data != NULL) {
                    cache_data->in_use++;
                }
            }
            pthread_mutex_unlock(&sv->lock);
            request_sendfile(rq);
            if (cache_data != NULL) {
                cache_data->in_use--;
            }
        }
        //pthread_mutex_unlock(&sv->lock);
        //request_sendfile(rq);
	/* read file, 
	 * fills data->file_buf with the file contents,
	 * data->file_size with file size. */
	/* send file to client */
	//request_sendfile(rq);
out:
	request_destroy(rq);
	file_data_free(data);
}

static void *
do_server_thread(void *arg)
{
	struct server *sv = (struct server *)arg;
	int connfd;

	while (1) {
		pthread_mutex_lock(&sv->mutex);
		while (sv->request_head == sv->request_tail) {
			/* buffer is empty */
			if (sv->exiting) {
				pthread_mutex_unlock(&sv->mutex);
				goto out;
			}
			pthread_cond_wait(&sv->cons_cond, &sv->mutex);
		}
		/* get request from tail */
		connfd = sv->conn_buf[sv->request_tail];
		/* consume request */
		sv->conn_buf[sv->request_tail] = -1;
		sv->request_tail = (sv->request_tail + 1) % sv->max_requests;
		
		pthread_cond_signal(&sv->prod_cond);
		pthread_mutex_unlock(&sv->mutex);
		/* now serve request */
		do_server_request(sv, connfd);
	}
out:
	return NULL;
}

/* entry point functions */

struct server *
server_init(int nr_threads, int max_requests, int max_cache_size)
{
	struct server *sv;
	int i;

	sv = Malloc(sizeof(struct server));
	sv->nr_threads = nr_threads;
	/* we add 1 because we queue at most max_request - 1 requests */
	sv->max_requests = max_requests + 1;
	sv->max_cache_size = max_cache_size;
	sv->exiting = 0;

	/* Lab 4: create queue of max_request size when max_requests > 0 */
	sv->conn_buf = Malloc(sizeof(*sv->conn_buf) * sv->max_requests);
	for (i = 0; i < sv->max_requests; i++) {
		sv->conn_buf[i] = -1;
	}
	sv->request_head = 0;
	sv->request_tail = 0;

	/* Lab 5: init server cache and limit its size to max_cache_size */
        // initialize our cache and lock
        struct cache_table* server_cache = cache_init();
        sv->server_cache = server_cache;
	/* Lab 4: create worker threads when nr_threads > 0 */
	pthread_mutex_init(&sv->mutex, NULL);
        pthread_mutex_init(&sv->lock, NULL);
	pthread_cond_init(&sv->prod_cond, NULL);
	pthread_cond_init(&sv->cons_cond, NULL);	
	sv->threads = Malloc(sizeof(pthread_t) * nr_threads);
	for (i = 0; i < nr_threads; i++) {
		SYS(pthread_create(&(sv->threads[i]), NULL, do_server_thread,
				   (void *)sv));
	}
	return sv;
}

void
server_request(struct server *sv, int connfd)
{
	if (sv->nr_threads == 0) { /* no worker threads */
		do_server_request(sv, connfd);
	} else {
		/*  Save the relevant info in a buffer and have one of the
		 *  worker threads do the work. */

		pthread_mutex_lock(&sv->mutex);
		while (((sv->request_head - sv->request_tail + sv->max_requests)
			% sv->max_requests) == (sv->max_requests - 1)) {
			/* buffer is full */
			pthread_cond_wait(&sv->prod_cond, &sv->mutex);
		}
		/* fill conn_buf with this request */
		assert(sv->conn_buf[sv->request_head] == -1);
		sv->conn_buf[sv->request_head] = connfd;
		sv->request_head = (sv->request_head + 1) % sv->max_requests;
		pthread_cond_signal(&sv->cons_cond);
		pthread_mutex_unlock(&sv->mutex);
	}
}

void
server_exit(struct server *sv)
{
	int i;
	/* when using one or more worker threads, use sv->exiting to indicate to
	 * these threads that the server is exiting. make sure to call
	 * pthread_join in this function so that the main server thread waits
	 * for all the worker threads to exit before exiting. */
	pthread_mutex_lock(&sv->mutex);
	sv->exiting = 1;
	pthread_cond_broadcast(&sv->cons_cond);
	pthread_mutex_unlock(&sv->mutex);
	for (i = 0; i < sv->nr_threads; i++) {
		pthread_join(sv->threads[i], NULL);
	}

	/* make sure to free any allocated resources */
	free(sv->conn_buf);
	free(sv->threads);
	free(sv);
}

// data is the requested_data and we will look into the has table to find if there is any 
struct cache_entry* cache_lookup(char* file_name, struct server *sv){
    int hash_number = hash(file_name);
    //printf("hash number is %d\n", hash_number);
    struct cache_entry* current_entry = sv->server_cache->cachetable[hash_number];
    while(current_entry->requested_data != NULL && current_entry != NULL) {
        if (strcmp(current_entry->requested_data->file_name, file_name) == 0){
            return current_entry;
        }
        current_entry = current_entry->next;
    }
    return NULL;
}

int cache_evict(struct server* sv, int coming_file_size){
    // Check if there is enough space for the coming file
    int size_left = sv->max_cache_size - sv->server_cache->total_file_size - coming_file_size;
    // do not need to evict
    if (size_left > 0) {
        return -1;
    } else {
        int copy_size_left;
        copy_size_left = size_left;
        struct lru_node* temp = sv->server_cache->lru_list->tail;
        while (temp != NULL && copy_size_left < 0) {
            struct cache_entry* temp_entry = cache_lookup(temp->file_name, sv);
            if (temp_entry->in_use == 1){
                return 0;
            }
            copy_size_left = copy_size_left + temp_entry->requested_data->file_size;
            temp = temp->prev;
        }
        while(size_left < 0) {
            char* file_to_evict = sv->server_cache->lru_list->tail->file_name;

            int hash_value = hash(file_to_evict);
            struct cache_entry* first_entry = sv->server_cache->cachetable[hash_value];
            struct cache_entry* entry_to_delete = cache_lookup(file_to_evict, sv);
            // The cache_entry is in use, do not evict it
            if (entry_to_delete->in_use == 1) {
                break;
            }
            struct cache_entry* temp_head = NULL;
            while (first_entry != entry_to_delete) { // find the location of entry_to_delete
                temp_head = first_entry;
                first_entry = first_entry->next;
            }
            
            // There is only one element in that entry or have multiple entries
            if (temp_head == NULL) { // The only one entry
                sv->server_cache->cachetable[hash_value] = entry_to_delete->next;
            } else { 
                temp_head->next = entry_to_delete->next; // Detach the entry_to_delete
            }
            sv->server_cache->total_file_size = sv->server_cache->total_file_size - entry_to_delete->requested_data->file_size;
            size_left = size_left + entry_to_delete->requested_data->file_size;
            // Update the LRU list by deleting the last node
            //free(entry_to_delete);
            file_data_free(entry_to_delete->requested_data);
            //free(entry_to_delete);
            struct lru_node* tail = lru_free_tail(sv->server_cache->lru_list);
            //file_data_free(entry_to_delete->requested_data);
            //free(tail->file_name);
            free(tail);
        }
        return -1;
    }
}

struct cache_entry* cache_insert(struct server* sv, struct file_data* file_data){
    //printf("cache insert!\n");
    char* file_name = file_data->file_name;
    int coming_file_size = file_data->file_size;
    if (coming_file_size > sv->max_cache_size) {
        return NULL;
    }
    //printf("The size fits in the cache\n");
    if (cache_lookup(file_data->file_name, sv) != NULL) {
        return NULL;
    }
    //printf("Not Inside the cache\n");
    int size_evicted = cache_evict(sv, coming_file_size);
    if (size_evicted == 0) {
        return NULL;
    }
    if (size_evicted == -1) {
        //printf("The file can be inserted\n");
        int hash_value = hash(file_name);
        struct cache_entry* new_entry = (struct cache_entry*)malloc(sizeof(struct cache_entry));
        new_entry->requested_data = file_data_init();
        new_entry->requested_data->file_name = strdup(file_name);
        new_entry->requested_data->file_buf = strdup(file_data->file_buf);
        new_entry->requested_data->file_size = coming_file_size;
        new_entry->in_use = 0;
        new_entry->next = sv->server_cache->cachetable[hash_value];
        sv->server_cache->cachetable[hash_value] = new_entry;
        sv->server_cache->total_file_size = sv->server_cache->total_file_size + coming_file_size;
        
        // Update the LRU list
        struct lru_node* new_node = (struct lru_node*)malloc(sizeof(struct lru_node));
        new_node->file_name = strdup(file_name);
        lru_push(new_node, sv->server_cache->lru_list);
        //printf("name of the file %s\n", sv->server_cache->lru_list->head->file_name);
        return new_entry;
    } else {
        return NULL;
    }
}

struct cache_table* cache_init() {
    struct cache_table* table;
    table = (struct cache_table*)malloc(sizeof(struct cache_table));
    table->total_file_size = 0; //initial empty cache
    table->size = CACHE_TABLE_SIZE; // How many entries does the cache table have
    
    table->lru_list = (struct lru_link *)malloc(sizeof(struct lru_link));
    table->lru_list->head = NULL;
    table->lru_list->tail = NULL;
    //for (int i = 0; i < CACHE_TABLE_SIZE; i++) {
        //struct cache_entry* temp = (struct cache_entry*)malloc(sizeof(struct cache_entry));
        table->cachetable = (struct cache_entry**)malloc(sizeof(struct cache_entry*) * CACHE_TABLE_SIZE);
        for (int i = 0; i < CACHE_TABLE_SIZE; i++) {
            struct cache_entry* temp = (struct cache_entry*)malloc(sizeof(struct cache_entry));
            table->cachetable[i] = temp;
            table->cachetable[i]->next = NULL;
            table->cachetable[i]->requested_data = NULL;
        }
    //}
    
    return table;
}

int hash (char* str) {
    unsigned long hash = 5381;
    int c;
    //int i = 0;
    while ((c = *str++)) {
        hash = ((hash << 10) + hash) + c;
    }
    return abs(hash % CACHE_TABLE_SIZE);
}