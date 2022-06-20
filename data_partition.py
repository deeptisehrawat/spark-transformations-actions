import json
import sys
import time

from pyspark import SparkContext


def execute_task2():
    if len(sys.argv) > 2:
        input_filepath = sys.argv[1]
        output_filepath = sys.argv[2]
        n_partition = int(sys.argv[3])
    else:
        input_filepath = './test_review.json'
        output_filepath = './output_task2.json'
        n_partition = 2

    sc = SparkContext('local[*]', 'Task 2')
    # remove logs
    sc.setLogLevel("ERROR")

    raw_rdd = sc.textFile(input_filepath)
    review_rdd = raw_rdd.map(json.loads).map(lambda s: (s['business_id'], 1)).persist()
    # print(review_rdd.collect())

    # default partitions
    start_time_default = time.time()
    default_rdd = review_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda s: (-s[1], s[0])).take(10)
    end_time_default = time.time()
    partitions_default = review_rdd.getNumPartitions()
    items_per_partition_default = review_rdd.glom().map(len).collect()

    # custom partitions
    start_time_custom = time.time()
    partition_rdd = review_rdd.partitionBy(n_partition, lambda s: hash(s) % n_partition)
    custom_rdd = partition_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda s: (-s[1], s[0])).take(10)
    end_time_custom = time.time()
    partitions_custom = partition_rdd.getNumPartitions()
    items_per_partition_custom = partition_rdd.glom().map(len).collect()

    # write performance
    output = {
        "default": {
            "n_partition": partitions_default,
            "n_items": items_per_partition_default,
            "exe_time": end_time_default - start_time_default,
        },
        "customized": {
            "n_partition": partitions_custom,
            "n_items": items_per_partition_custom,
            "exe_time": end_time_custom - start_time_custom,
        },
    }

    with open(output_filepath, "w+") as output_file:
        json.dump(output, output_file)


if __name__ == '__main__':
    execute_task2()
