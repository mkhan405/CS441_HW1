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

## Video Discussion Link
Youtube:

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

With statistics of the word corupus computed, we need to compute the sliding window data samples for the text corpus. The Mapper still utilizes the `TextInputFormat` input file format, and is given the `LongWritable` key and `Text` value containing the line data. First, the sentences are split by punctuation characters and each sentence is then split by whitespace characters. Then, each word is converted to it's BPE token, leading to the following result:

```
hello world. This is a test.
-> ["hello world", "This is a test"]
-> [["hello", "world"], ["This", "is", "a", "test"]]
-> [[101.0, 202.0], [303.0, 301.0, 202.0, 404.0]]
```

Then, for this sequence, sliding window pairs are computed based on the window size and stride. The window size and stride can be specified in the `application.conf` file under the `training-conf` section. With a window size of 10 and stride 2, these are the samples that would be generated

```
    [101.0, 202.0, 303.0, 301.0, 202.0] ->
    [101.0, 202.0, 303.0, 301.0, 202.0, 0, 0, 0, 0, 0],
    [303.0, 301.0, 202.0, 0, 0, 0, 0, 0, 0, 0],
    [202.0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```
The 0s are the padidng token that is inserted when the size of the generated window is shorter than the `window_size` in `application.conf`.

Each of these samples is then sent to the reducer with the following key-value pair: `<<shard_filename>-<LongWritableKey>-<index>-<sample_index>, [101.0, 202.0, 303.0, 301.0, 202.0, 0, 0, 0, 0, 0]>`. The reason for this is:
- The samples in the reducer will be ordered by shard
- The samples will be ordered by their order in the respective shard
- The samples will be ordered by which sentence they were in their line (e.g. 1st, 2nd, 3rd sentence)
- The samples will finally be ordered by the sample index (e.g. 1st, 2nd, 3rd sample)

This will be necessary in maintaining the same relative order as in the original text, which will be vital for the training phase. The vectors are also sent to the reducer as a custom `VectorWritable` type, which will serialize the vector to a ";"-deliminated string and contains mathematical helper functions which will be used in the training phase.

The reducer then receives the following set: `<<shard_filename>-<LongWritableKey>-<index>-<sample_index>, [[101.0, 202.0, 303.0, 301.0, 202.0]]>`. Although the reducer will receive a list of sliding window samples, since each mapper is only assigned one shard, the sample with this particular key will only appear once and can be written straight to the output file. The result is then a file with the following format:

```
shard_0-0-0-1	438.0;3055.0;1962.0;82.0;98148.0;48210.0;4065.0;1527.0;33169.0;2940.0
shard_0-145-0-1	11099.0;92204.0;15357.0;811.0;1527.0;15357.0;1105.0;258.0;21470.0;33169.0
shard_0-255-0-1	548.0;911.0;349.0;3725.0;55674.0;11.0;8248.0;906.0;34360.0;7072.0
```

### Training Data on Token Embeddings

To parallelize the training of token embeddings, the samples computed in the previous step are sharded in the same manner as the original input file and assigned to each mapper. Each mapper will implement and fit it's own `Word2Vec` model. However, since the `deeplearning4j` model requires an iterator to fit the model, a custom iterator was implemented which will iterate over the values assigned to the mapper based on the `context` object provided by the Hadoop API:

```scala
    override def hasNext(): Boolean = {
      // Return true if currentLine already has a value
      if (currentLine != null) {
        return true
      }
      // Try to get the next line from the Mapper context
      try {
        val value = context.getCurrentValue

        // If a value is present, process it
        if (value != null) {
          // Extract the token IDs from the value, convert them into a space-separated string
          val tokenIDs = value.toString.split("\t").last
          currentLine = tokenIDs.split(";").mkString(" ")
          true
        } else {
          // No more lines to read
          hasMoreLines = false
          false
        }
        // Move to the next key-value pair in the context
        context.nextKeyValue()
      } catch {
        case _: Exception =>
          // Handle any exceptions that occur and set hasMoreLines to false
          hasMoreLines = false
          false
      }
    }
```
The implementation can found in `com.mapreduce.tasks.Embeddings.EmbeddingMapper.HadoopLineIterator`, which extends the `SentenceIterator` interface provided by `deeplearning4j` to help fit the model. In the same manner as the `TextInputFileFormat`, the iterator goes through each line of its input document. Here is how the it is processed:

```
shard_0-0-0-1	438.0;3055.0;1962.0;82.0;98148.0;48210.0;4065.0;1527.0;33169.0;2940.0
-> split by space: ["shard_0-0-0-1", "438.0;3055.0;1962.0;82.0;98148.0;48210.0;4065.0;1527.0;33169.0;2940.0"]
-> split the tokens and write as a space-delimited string:
"438.0;3055.0;1962.0;82.0;98148.0;48210.0;4065.0;1527.0;33169.0;2940.0" -> "438.0 3055.0 1962.0 82.0 98148.0 48210.0 4065.0 1527.0 33169.0 2940.0"
```

Each Word2Vec model will process each sentence and iterate this way over its input shard, and with the input given as space delimeted Byte Pair Encoding Integer IDs, each Integer ID counts as a unique word for the embedding model. Once the fit is completed, we iterate over all the unique words encountered by the model and send the output to the reducer:

```scala
      val vocab = word2Vec.getVocab.words() // Get the vocabulary learned by the model
      vocab.forEach { word =>
        // For each word in the vocabulary, get its embedding and similar words
        val embedding = word2Vec.getWordVector(word).map(_.toFloat).mkString(";") // Convert embedding to a string
        val similarWords = word2Vec.wordsNearest(word, 5).toArray().mkString(";") // Find the nearest words
        // Output the word and its corresponding embedding and similar words to context
        context.write(new Text(word), new Text(s"${embedding}-${similarWords}")) // Write output to context
      }
```

Here is an explanation of the output:
- Key: Unique BPE Token ID for a word in the input text
- Embedding: ";"-delimited string representing the embedding vector computed
- Similar Words: ";"-delimited string representing the the top 5 closest BPE Integer IDs determined by the model

In the reducer, the following output is received:
`<BPE_TOKEN,["123;123;123;-SIM_BPE_TOKEN,SIM_BPE_TOKEN,SIM_BPE_TOKEN","123;123;123;-SIM_BPE_TOKEN,SIM_BPE_TOKEN,SIM_BPE_TOKEN"]>`

The reducer performs the following transformations:
- Splits each element in the value list by "-" and extracts a list of `VectorWritable` objects, representing each embedding vector computed by each mapper
- Extracts an array of strings containing all the BPE tokens similar to the key and chooses the top 3 most frequently occuring BPE tokens in this list
- Decodes each BPE Token ID back to it's textual representaiton
- Calculates the average from the list of `VectorWritable` objects to calculate the final embedding vector

This leaves us with the following output in the `embedding.csv` file:
```
was,0.065905005;0.14959455;0.068675935;0.14571849;0.0060369205,for, reatment, he
```
Where the first item is a word from the vocabulary, a ;-delimited string representing the embedding vector, followed by the three most similar tokens to the key
