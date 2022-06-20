import json
import sys
import time

from pyspark import SparkContext


def execute_task3():
    if len(sys.argv) > 3:
        review_filepath = sys.argv[1]
        business_filepath = sys.argv[2]
        output_filepath_question_a = sys.argv[3]
        output_filepath_question_b = sys.argv[4]
    else:
        review_filepath = './test_review.json'
        business_filepath = './business.json'
        output_filepath_question_a = './output_file_question_a.txt'
        output_filepath_question_b = './output_file_question_b.json'

    sc = SparkContext('local[*]', 'Task 2')
    # remove logs
    sc.setLogLevel("ERROR")

    review_rdd = sc.textFile(review_filepath).map(json.loads).map(lambda s: (s['business_id'], s['stars'])).persist()
    business_rdd = sc.textFile(business_filepath).map(json.loads).map(lambda s: (s['business_id'], s['city'])).persist()

    # print(review_rdd.collect())
    # print(business_rdd.collect())
    # n_business = business_rdd.distinct().count()
    # print(n_business)

    #  question a: average stars for each city
    # get key, value pairs of city and reviews
    joined_rdd = review_rdd.join(business_rdd).map(lambda s: (s[1][1], s[1][0]))
    # print(joined_rdd.collect())
    # print(joined_rdd.count())

    # get averaged city data
    zero_value = (0, 0)
    city_avg_rdd = joined_rdd\
        .aggregateByKey(zero_value,
                        lambda curr_sc, next_sc: (curr_sc[0] + next_sc, curr_sc[1] + 1),
                        lambda curr_sc, part_sc: (curr_sc[0] + part_sc[0], curr_sc[1] + part_sc[1]))\
        .mapValues(lambda s: s[0] / s[1])

    # print(city_avg_rdd.collect())
    # print(city_avg_rdd.count())

    # question b: print top 10 cities with highest stars
    # method 1: sort in python
    start_time_m1 = time.time()
    py_top10_rdd = sorted(city_avg_rdd.collect(), key=lambda s: (-s[1], s[0]))
    py_top10_rdd = py_top10_rdd[:10]
    for row in py_top10_rdd:
        print(row[0])
    time_m1 = time.time() - start_time_m1

    # method 2: sort in spark
    start_time_m2 = time.time()
    city_avg_rdd = city_avg_rdd.sortBy(lambda s: (-s[1], s[0]))
    spark_top10_rdd = city_avg_rdd.take(10)
    for row in spark_top10_rdd:
        print(row[0])
    time_m2 = time.time() - start_time_m2

    # write output of question a
    with open(output_filepath_question_a, "w+") as output_file_a:
        output_file_a.write("city,stars\n")
        for row in city_avg_rdd.collect():
            output_file_a.write(row[0] + "," + str(row[1]) + "\n")

    # write performance
    output = {
        "m1": time_m1,
        "m2": time_m2,
    }
    with open(output_filepath_question_b, "w+") as output_file_b:
        json.dump(output, output_file_b)

    # My prediction: my Python code will run faster than my Spark code on a large dataset on Vocareum:
    # Reason 1: Python's sorting function is very optimised.


if __name__ == '__main__':
    execute_task3()
