import json
import sys

from pyspark import SparkContext


def execute_task1():
    if len(sys.argv) > 1:
        input_filepath = sys.argv[1]
        output_filepath = sys.argv[2]
    else:
        input_filepath = './test_review.json'
        output_filepath = './output_task1.json'

    sc = SparkContext('local[*]', 'Task 1')
    # remove logs
    sc.setLogLevel("ERROR")

    raw_rdd = sc.textFile(input_filepath)
    review_rdd = raw_rdd.map(json.loads).map(lambda s: (s['date'], s['user_id'], s['business_id'])).persist()
    # print(review_rdd.collect())

    # question A
    n_review = review_rdd.count()

    # question B
    n_review_2018 = review_rdd.filter(lambda s: s[0][:4] == '2018').count()

    # question C
    user_rdd = review_rdd.map(lambda s: (s[1], 1))
    n_user = user_rdd.distinct().count()

    # question D
    top10_user = user_rdd.reduceByKey(lambda a, b: a+b).sortBy(lambda s: (-s[1], s[0])).take(10)

    # question E
    business_rdd = review_rdd.map(lambda s: (s[2], 1))
    n_business = business_rdd.distinct().count()

    # question F
    top10_business = business_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda s: (-s[1], s[0])).take(10)

    output = {
        "n_review": n_review,
        "n_review_2018": n_review_2018,
        "n_user": n_user,
        "top10_user": top10_user,
        "n_business": n_business,
        "top10_business": top10_business,
    }

    with open(output_filepath, "w+") as output_file:
        json.dump(output, output_file)

    # print result
    # print('n_review ', n_review)
    # print('n_review_2018 ', n_review_2018)
    # print('n_user ', n_user)
    # print('top10_user ', top10_user)
    # print('n_business ', n_business)
    # print('top10_business ', top10_business)


if __name__ == '__main__':
    execute_task1()
