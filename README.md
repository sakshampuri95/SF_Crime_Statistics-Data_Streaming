# SF_Crime_Statistics-Data_Streaming

Q1) How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Ans) Changing spark session properties will have a direct impact on the throughput and latency. It can increase or decrease throughput and latency sepending on the properties being set.
For ex: Increasing spark cores will improve the throughput per batch.

Q2) What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
Ans) Following are some of the properties which can optimize the performance of the application:
    spark.executor.instances = 2
    spark.executor.cores = 2 ( Based on the hardware available on local setup, this configuration gave the best throughput. )
    spark.sql.shuffle.partitions = number of total cores / tasks to work with. i.e. 4
    spark.streaming.kafka.maxrateperpartition = 50
