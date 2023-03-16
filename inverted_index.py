#!/usr/bin/env python

import argparse
import json
import os
from pyspark import SparkContext

def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce inverted index (Problem 1)')
    parser.add_argument('-d', help='path to data file', default='./../data/books_small.json')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default='output')
    return parser.parse_args()

# Feel free to create more mappers and reducers
def mapper1(record):
    words = record[1].split()
    for word in words:
        yield (word, [record[0]])

def reducer1(a, b):
    for word in a:
        if word not in b:
            b += a
    return b

def mapper2(record):
    return (record[0], record[1])

def reducer2(a, b):
    return a + b


# Do not modify except defining inverted_index_result
def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d, 'r') as infile:
        data = [json.loads(line) for line in infile]



    # TODO: build your pipeline
    inverted_index_result = sc.parallelize(data, args.n).flatMap(mapper1).reduceByKey(reducer1).map(mapper2).reduceByKey(reducer2).collect()


    sc.stop()
    """
    output_inverted_index.json should be of the format -
    [
        [
            "dust",
            [
                "melville-moby_dick.txt"
            ]
        ],
        [
            "very",
            [
                "austen-emma.txt",
                "chesterton-thursday.txt",
                "edgeworth-parents.txt"
            ]
        ]
    ]

    """

    if not os.path.exists(args.o):
        os.makedirs(args.o)

    with open(args.o + '/output_inverted_index.json', 'w') as outfile:
        json.dump(inverted_index_result, outfile, indent=4)



if __name__ == '__main__':
    main()
