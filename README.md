Using file granularity caching for web server
Implemented LRU policy for caching replacement with double linked list
Implemented hash table for lookup cached files {key: filename, data: file datat fetched from the disk}
Evict other files if new file will exceed the max
