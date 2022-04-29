import findspark

findspark.init()

from flask import Flask, request, redirect, url_for, jsonify
import asyncio
import sys
import json
import os
import pyspark

# Part III

# Get line lenghts and count
f1 = 'output/part-00000'
f2 = 'output/part-00001'

word_dict = dict()
with open(f1) as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        len_count_arr = line[1:len(line) - 1].split(', ')
        key = len_count_arr[0]
        value = int(len_count_arr[1])
        word_dict[key] = value

with open(f2) as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        len_count_arr = line[1:len(line) - 1].split(', ')
        key = len_count_arr[0]
        value = int(len_count_arr[1])
        word_dict[key] = value

# Part IV
def mapFuncOxford(x):
    word = x[0]
    sen = x[1]
    weights = x[2]
    if word in sen:
        weight = 0
        for char in sen:
            if char.isAlpha() and char in weights.keys():
                weight+= weights[char]
        return (word, (sen, weight))
    else:
        return (word, (sen, 0))

def reduceFuncOxford(v1, v2):
    if v2[1] > v1[1]:
        return v2
    else:
        return v1

input_text = 'input.txt'
war_peace_lines = []
with open(input_text) as f:
    war_peace_lines = f.readlines()
war_peace_lines = [l.strip() for l in war_peace_lines]

def check_done():
    filesize = os.path.getsize("result.json")
    if filesize == 0:
        return False
    else:
        return True

# start flask app
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route("/lengthCounts", methods=['GET'])
def lengthCounts():
    return jsonify(word_dict)

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    words = data['wordlist']
    weights = data['weights']
    collection = [(word,s,weights) for word in words for s in war_peace_lines]

    rdd_tuples = sc.parallelize(collection)

    sen_val = rdd_tuples.map(mapFuncOxford).reduceByKey(reduceFuncOxford).collect()

    result = dict()
    for sen in sent_val:
        word = sen[0]
        val = sen[1]
        sentence = val[0]
        result[word] = sentence

    with open('./result.json','w') as f:
        json.dump(result, f)
    return "OK"

@app.route('/result', methods=['GET'])
def result():
    if check_done():
        with open('./result.json', 'r') as f:
            x = json.load(f)
        return jsonify(x)
    else:
        return "Not done yet"

app.run(host='0.0.0.0', port=80)
