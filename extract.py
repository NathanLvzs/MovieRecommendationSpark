#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 4, 2015

@author: NathanLVZS
"""

# already map the user id to new user id in range(1, #users+1)
# the user id in the large dataset is in an increasing order
# seems not necessary to map movie id to new id

import os
import time
import random
import cPickle as pickle

# 20M Dataset: 138,000 users, 27,278 movies
num_user_all = 138000
num_movie_all = 27000

# num_rating2sample = 200000#141191
threshold_rate_movies = 20
num_user_sample = 3000
num_movie_sample = 10000

# 100,000 ratings and 2,200 tag applications applied to 9,000 movies by 700 users. 
# Users were selected at random for inclusion. All selected users had rated at least 20 movies. 
# http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html
# use movie genre to recommend, jaccard similarity?

filename_ratingscsv = "ratings.csv"
filename_ratings_small_csv = "ratings_small.csv"
filename_movieids = "movieids"
filename_movieinfo = "movieinfo"
filename_moviesample = "moviesample"
filename_usersample = "usersample"
filename_useractualsample = "useractualsample"

start_time = time.time()


def save_data_pickle(filename, data):
    with open(filename, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

def load_data_pickle(filename):
    content = None
    with open(filename, "rb") as f:
        content = pickle.load(f)
    return content

data_movies = {}
ids_movie = None
if os.path.exists(filename_movieids):
    ids_movie = load_data_pickle(filename_movieids)
    # data_movies = load_data_pickle(filename_movieinfo)
else:
    csv_movies = "movies.csv"
    with open(csv_movies, "r") as file_mov:
        for line_num, line in enumerate(file_mov, 1):
            if line_num == 1:
                continue
            seps = line.replace("\n", "").split(",", 2)
            id_mov = int(seps[0])
            data_movies[id_mov] = (seps[1], seps[2].split("|"))
    save_data_pickle(filename_movieinfo, data_movies)
    ids_movie = set(data_movies.keys())
    print "number of movies: %d" % (len(ids_movie))
    save_data_pickle(filename_movieids, ids_movie)

# sample from movies
movie_sampled = None
user_sampled = None
if os.path.exists(filename_moviesample):
    movie_sampled = load_data_pickle(filename_moviesample)
else:
    movie_sampled = set(random.sample(ids_movie, num_movie_sample))
    save_data_pickle(filename_moviesample, movie_sampled)
if os.path.exists(filename_usersample):
    user_sampled = load_data_pickle(filename_usersample)
else:
    user_sampled = set(random.sample(set(range(1, num_user_all+1)), num_user_sample))
    save_data_pickle(filename_usersample, user_sampled)


outfile_rating_small = open(filename_ratings_small_csv, 'w')
user_actualsampled = set()
last_userid = 0
user_rated_movies = []

userid_mapped = 1# for user id mapping

with open(filename_ratingscsv, 'r') as f:
    for line_num, line in enumerate(f, 1):
        # print "line_%d:\t%s" % (line_num, line)
        seps = line.split(",")
        user_id = seps[0]
        try:
            user_id = int(user_id)
        except ValueError:
            continue
        if user_id not in user_sampled:
            continue
        movie_id = int(seps[1])
        rating = float(seps[2])
        if user_id != last_userid:
            if len(user_rated_movies) >= threshold_rate_movies:
                outfile_rating_small.write("\n".join(user_rated_movies) + "\n")
                user_actualsampled.add(user_id)
                userid_mapped = userid_mapped + 1# for user id mapping
            user_rated_movies = []
            last_userid = user_id
        else:
            if movie_id in movie_sampled:
                user_rated_movies.append("%d,%d,%.1f" % (userid_mapped, movie_id, rating))# for user id mapping
                # user_rated_movies.append("%d,%d,%.1f" % (user_id, movie_id, rating))
if len(user_rated_movies) >= threshold_rate_movies:
    outfile_rating_small.write("\n".join(user_rated_movies) + "\n")
    user_actualsampled.add(user_id)
outfile_rating_small.close()

print "sampled user count: %d" % len(user_actualsampled)#1729

if os.path.exists(filename_useractualsample):
    os.remove(filename_useractualsample)
save_data_pickle(filename_useractualsample, user_actualsampled)

print("finish in --- %s seconds ---" % (time.time() - start_time))


