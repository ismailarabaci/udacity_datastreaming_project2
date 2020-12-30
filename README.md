# udacity_datastreaming_project2

## Question 1: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It seems that `maxOffsetPerTrigger` is the most relevant parameter. The dataset in this project is small enough to be consumed and aggregated in a single batch so setting a (low) value `maxOffsetPerTrigger` only introduces more overhead. For low enough values this leads to reduced throughput and increased latency.

## Question 2: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

I have tried various combinations and found that the default values (`maxOffsetPerTrigger` is unlimited) work best. I used the SparkUI to monitor the jobs.
