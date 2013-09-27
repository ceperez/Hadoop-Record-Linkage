Hadoop-Word-Similarity
===================

Hadoop job to find the US place name most similar to a UK place name using a distributed cache. It's a specific instance of the general problem of computing the similarity between an input set of sequences and a fixed target one, possibly followed by a selection operation.

A sequential implementation has O(N^2) complexity, which can be reduced to a theoretical O(N) (in the limit of one node per input) if implemented as MapReduce.

Uses a normalised (in [0,1]) [levenshtein distance](http://en.wikipedia.org/wiki/Levenshtein_distance) as similarity metric. There's also an alternative implementation as a normalised bigram [dice's coefficient](http://en.wikipedia.org/wiki/S%C3%B8rensen_similarity_index).

Note that for the particular problem at hand (finding the closest match) the current implementation is not the most efficient as the computation of the maximum similarity can be implemented as a single operation in the mapper instead of delegating it to the reducer (which would then just output the result).

The US gazetteer is taken from [here](http://geonames.usgs.gov/domestic/download_data.htm) and split into four files, and the UK one from [here](http://ukgaz.ben-daglish.net/cgi-bin/ukgaz.cgi?page=download) as is.

Running it
===================

From the project root after running  `maven clean install`

```shell
hadoop jar target/hadoop-ws.jar com.fluent.hadoop.Word_Similarity -files src/main/resources/in/metadata/places.csv src/main/resources/in/data target/out
```


Improvements
===================

* Use phonetic similiarity instead of orthographic one

* Use bigram dice coefficient instead of minimum edit distance

* Cache substring distances

* Multi-thread the similarity computation

* Apply [locally sensitive hashing](http://en.wikipedia.org/wiki/Locality-sensitive_hashing) as an intermediate step to shortlist the most similar target sequences thus avoiding some of the expensive similarity computations

* Maybe simple in Scalding?

Variations
==============

*  output only matches above a threshold
