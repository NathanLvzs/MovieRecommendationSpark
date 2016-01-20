#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 4, 2015

@author: NathanLVZS
"""
# check whether the user id and movie id is in an increasing order

import time
from collections import defaultdict

start_time = time.time()
filename_ratingscsv = "ratings.csv"

last_userid = 0
last_movieid = 0
rating_dict = defaultdict(lambda: 0)
user_order_flag = True
user_inc1_flag = True
movie_order_flag = True
loguser = []
logmovie = []
largest_userid = 0
num_rows = 0

with open(filename_ratingscsv, 'r') as f:
    for line_num, line in enumerate(f, 1):
        seps = line.split(",")
        user_id = seps[0]
        try:
            user_id = int(user_id)
        except ValueError:
            continue
        movie_id = int(seps[1])
        if user_id < last_userid:
            user_order_flag = False
        if user_id not in [last_userid, last_userid+1]:
            user_inc1_flag = False
            loguser.append((last_userid, user_id, line_num))
        if user_id == last_userid and movie_id < last_movieid:
            movie_order_flag = False
            logmovie.append((last_movieid, movie_id, line_num))
        rating = float(seps[2])
        rating_dict[rating] += 1
        last_userid = user_id
        last_movieid = movie_id
        largest_userid = user_id
        num_rows = line_num

print len(loguser)#0
print len(logmovie)#0
print "user ids are in an increasing order: %s" % (str(user_order_flag))#True
print "user id incremental step is 1: %s" % (str(user_inc1_flag))#True
print "movie in an increasing order: %s" % (str(movie_order_flag))#True

# {3.5: 2200156, 4.5: 1534824, 2.0: 1430997, 3.0: 4291193, 4.0: 5561926, 5.0: 2898660, 2.5: 883398, 1.0: 680732, 1.5: 279252, 0.5: 239125})
print rating_dict
print largest_userid#138493
print num_rows#20000264

print("finish in --- %s seconds ---" % (time.time() - start_time))#about 57seconds
