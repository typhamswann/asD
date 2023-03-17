#!/usr/bin/env python

import argparse
import json
import os
from os.path import dirname, realpath

from pyspark import SparkContext


def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce join (Problem 2)')
    parser.add_argument('-d', help='path to data file', default='./../data/records.json')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default='output')
    return parser.parse_args()


# Feel free to create more mappers and reducers.
def mapper1(record):
    return (record[2], [record])

def reducer1(a, b):
    return a + b

def reducer2(a,b):
    comp = record[1]

def mapper2(record):
    for c in record[1]:
        for d in record[1]:
            yield c + d

def filter1(record):
    return (record[0], record[17]) == ('rele', 'disp')

def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d, 'r') as infile:
        data = [json.loads(line) for line in infile]
    
    # TODO: build your pipeline
    join_result = sc.parallelize(data, args.n).map(mapper1).reduceByKey(reducer1).flatMap(mapper2).filter(filter1).collect()

    sc.stop()

    if not os.path.exists(args.o):
        os.makedirs(args.o)

    with open(args.o + '/output_join.json', 'w') as outfile:
        json.dump(join_result, outfile, indent=4)


if __name__ == '__main__':
    main()
