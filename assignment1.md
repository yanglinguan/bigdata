# Assignment 1 

## Question 1

### Pairs Implementation 

Pairs implementation is separated into two MapReduce jobs.  
For the first job, the goal is to count the total number of lines of input file,
and the number of lines that the unique words appear in the file.
The input records are key-value pairs where the values are the lines of the original input file.
The key of intermediate key-value pairs is either "\*" or a word, 
and the value of intermediate key-value pairs is 1 which means a word appears 1 line. 
The key, "\*", denotes that its value is the number of lines.
The final output records are key-value pairs that the key is the either "\*" or a word, and 
the value is the either the total number of lines or number of lines a word appears.
For example, a final record, (a, 123), means word 'a' appears in 123 lines. 
(*, 123) means the file has 123 lines in total.  
For the second job, the goal is to compute the PMI value for unique pair of words by using the result from the first job.
The input records are the same as the first job.
In the Map phase, the key of the intermediate key-value pairs is the unique word pairs,
 and the value is 1 which means this pair appears in 1 line.
In the reduce phase, in the setup function, it will load the result of the first job, and store them in a map. 
In reduce function, it will compute the PMI for the pair of words. The final output records are key-value pairs.
The key is pair of words and the value is also a pair which records the PMI and number of occurrences of the pair of words.
For example, ((a, b), (1.0, 10)) means that pair (a, b) appears in 10 lines and the PMI value is 1.0.

### Stripes Implementation
Pairs implementation is separated into two MapReduce jobs.  
The first job is the same as the first job of Pairs implementation.
For the second job, the input records is the key-value pairs that the value is the 
line of the original input file. The key of intermediate key-value pairs is a word, 
and the value is a map that the key is the co-occur word and value is the number of times of co-occur.
Same as the second job of Pairs implementation, in the setup function of reduce, it will load the results of the first job.
Then, in the reduce function, it will compute the PMI. 
The final output records are key-value pairs. The key is a word, and the value is 
a map that the key is the co-occur word and value is a pairs of PMI and number times of two words co-occur.
For example, (a, {(b, (1.2, 10)), (c, (2.3, 15))}) means that a and b co-occur 10 times and the PMI value is 1.2.
Similarly, a and c co-occur 15 times and PMI is 2.3.

## Question 2
The experiments run on linux.student.cs.uwaterloo.ca.  
The running time of the complete pairs implementation is about 51.269 seconds.  
The running time of the complete stripes implementation is about 20.155 seconds.

## Question 3
The experiments run on linux.student.cs.uwaterloo.ca  
Without any combiner, the running time of the complete pairs implementation is about 62.159 seconds.  
Without any combiner, the running time of the complete stripes implementation is about 25.62 seconds.

## Question 4
The result of this question by running the script is 77198  308792 2482188.  
The number of distinct PMI pairs is 77198.
## Question 5
The highest PMI pairs, using -threshold 10, are
* (maine, anjou)	(3.6331422, 12.0)  
* (anjou, maine)	(3.6331422, 12.0)  

The lowest PMI pairs, using -threshold 10, are:  
* (thy, you)	(-1.5303967, 11.0)  
* (you, thy)	(-1.5303967, 11.0)

## Question 6
The three words that have the highest PMI with "tears", using -threshold 10, are "shed", "salt", and "eyes".
* (tears, shed)	(2.1117902, 15.0)  
* (tears, salt)	(2.052812, 11.0)  
* (tears, eyes)	(1.165167, 23.0)  

The three words that have the highest PMI with "death", using -threshold 10, are "father's", "die", and "life".
* (death, father's)	(1.120252, 21.0)  
* (death, die)	(0.7541594, 18.0)  
* (death, life)	(0.7381346, 31.0)  


## Question 7
In the Wikipedia dataset,  the five words that have the highest PMI with "hockey", 
using -threshold 50 are "defenceman", "winger", "goaltender", "ice", and "nhl".
The co-occur times are 147, 185, 198, 2002, and 940 respectively.
* (hockey, defenceman)	(2.4030268, 147.0)  
* (hockey, winger)	(2.3863757, 185.0)  
* (hockey, goaltender)	(2.2434428, 198.0)  
* (hockey, ice)	(2.195185, 2002.0)  
* (hockey, nhl)	(1.9864639, 940.0)  


## Question 8
In the Wikipedia dataset,  the five words that have the highest PMI with "data", 
using -threshold 50 are "storage", "database", "disk", "stored", and "processing".
The co-occur times are 100, 97, 67, 65, and 57 respectively. 
* (data, storage)	(1.9796829, 100.0)  
* (data, database)	(1.8992721, 97.0)  
* (data, disk)	(1.7935462, 67.0)  
* (data, stored)	(1.7868547, 65.0)  
* (data, processing)	(1.6476576, 57.0)  