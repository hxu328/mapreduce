CC=gcc

mapreduce: main.c mapreduce.c hashmap.c mapreduce.h hashmap.h
	$(CC) -o mapreduce main.c mapreduce.c hashmap.c -lpthread -Wall -Werror

test:
	./mapreduce 4.txt 1.txt 2.txt 3.txt four

clean:
	rm -f mapreduce
	
