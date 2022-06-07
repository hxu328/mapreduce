# mapreduce
implement a correct and efficient MapReduce framework using threads and related functions
original paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## general ideas
As from the original paper: "Map(), written by the user, takes an input pair and produces a set of intermediate key/value pairs. The MapReduce library groups together all intermediate values associated with the same intermediate key K and passes them to the Reduce() function."

"The Reduce() function, also written by the user, accepts an intermediate key K and a set of values for that key. It merges together these values to form a possibly smaller set of values; typically just zero or one output value is produced per Reduce() invocation. The intermediate values are supplied to the user's reduce function via an iterator."

## Code Overview
1. mapreduce.h - This header file specifies exactly what you build for your MapReduce library.
2. mapreduce.c - This is a functional, efficient concurrent implementation of MapReduce.
3. hashmap.h - This header file specifies the interface for the HashMap
4. hashmap.c - This is a functional implementation of a HashMap.
5. main.c - runs the whole program.
