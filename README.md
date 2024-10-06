# Homework #1

## Student Information
- First Name: Mohammad Shayan
- Last Name: Khan
- UIN: 667707825
- UIC Email: mkhan405@uic.edu

## Prerequisites
- Ensure Scala 3.3.3 is installed
- Ensure Apache Hadoop (v. 3.3.6) is running on your system 

## Installation
- Clone the github repository
- In IntelliJ, go to `File > New > Project From Existing Sources` and open the project
- Navigate to project root and to `src/main/resources/application.conf` and make the following change to setup the input/output file paths:
  
  ```
  paths {
    base_dir="<PROJECT_CLONE_LOCATION>/CS441_HW1/src/main/resources"
  }

  ```
- Navigate to `Build > Build Project` to build the project and install relevant dependencies
- Go to `src/main/scala/com/mapreduce/task/Driver` and run the `main` function to start the program

## Project Implementation

### Sharding the Dataset
The dataset was sharded by dividing the original text into even sized "chunks". The number of these shards by the `job-conf.num_jobs` configuration parameter in `application.conf`, where each shard will have approximately the same size. Each shard is then given to a mapper for parallel processing. This task is performed by the `ShardDriver.shardFile` method located in `com.mapreduce.task.utils.ShardDriver`. As an example, if the total file size is 200mb and `job-conf.num_jobs` is set to 2, the total size for each shard is: 200/2 = 100 mb.

### Mappers/Reducers Implemented

- Counting Words: WordCountMapper/WordCountReducer
- Sldiing Window Samples: SlidingWindowMapper/SlidingWindowReducer
- Token Embeddings: EmbeddingMapper/EmbeddingReducer

### Counting Words

Each mapper is assigned one of the shards generated in the previous step. The Mapper utilizes the `TextInputFormat`, where the mapper is fed in the `LongWritable` Key representing the unique offset of the file and a `Text` object representing the line that was read. To count the words in this sentence, the line was split by the whitespace character and converted to a list `"hello world" -> ["hello", "world"]`. Using the `foreach` higher-order function to iterate over the collection, the word's Byte Pair Encoding is generated in the form of `Array[Float]` (e.g. `"hello" -> [123.0, 43.0, 545.0]`). These BPEs are then transformed into a string with a ";" delimeter (e.g. `"123.0;43.0;545.0"`). The ouput to the reducer is as follows:

```
<"hello", "1,123.0;43.0;454.0"> 
```
"hello" is the key or the word this token is generated for, 1 represents the frequency of this word, followed by the list of floats representing the BPE for the word.

In the reducer, the following the key-value set is received:

```
<"hello", ["1,123.0;43.0;454.0", "1,123.0;43.0;454.0"]> 
```

To calculate the word count, each element in the list is first split by the "," and then the count is extracted. Since each item starts with 1, the head element of the list is extracted everytime:

New List:
```
[
    ["1", "123.0;43.0;454.0"],
    ["1", "123.0;43.0;454.0"]
]
```

```scala
      // Split each record by comma and create a list of metrics
      val metrics = records.map(_.toString.split(",").toList)

      // Sum the first element (count) of each metric to get the total count for the word
      val count = metrics.map(_.head.toInt).sum

```

Since the BPE encoding for a particular token is guarunteed to have the same result assuming the same encoding, the BPE tokens from the first element are retrieved:
```scala
      val tokenIDs = metrics.head.last
      // Result written to output file
      context.write(new Text(s"${key},${count},${tokenIDs}"), NullWritable.get())
```
The output is then written to the output file (e.g. `"hello",3,123.0;43.0;454.0`)

In this case:
  - "hello" is the word.
  - 3 is the total frequency of the word across all shards.
  - "123.0;43.0;454.0" is the corresponding BPE vector for the word.


### Computing Sliding Window Pairs

With statistics of the word corupus computed, we need to compute the sliding window data samples for the text corpus. The Mapper still utilizes the `TextInputFormat` input file format, and is given the `LongWritable` key and `Text` value containing the line data. First, the input is split by whitespace and the BPE's are then generated:

```
hello world this is a test
-> ["hello", "world", "this", "is", "a", "test"]
-> [101.0, 202.0, 303.0, 301.0, 202.0]
```
