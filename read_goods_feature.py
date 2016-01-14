#!/usr/bin/env python
# coding=utf-8

import simplejson
import math
import sys 
import redis
from DBsetting import *


reload(sys)
sys.setdefaultencoding('utf8')

def connect_redis():
    redisClient=redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    return redisClient

def shop_write_to_redis(client, term_ii, term_wt, term_idf, term_UB):
    sufix = '_shop'
    for term in term_idf:
        savestr = term + sufix
        inverted_index_list = term_ii[term]
        wt_list = term_wt[term]
        idf = term_idf[term]
        UB = term_UB[term]
        new_list = [inverted_index_list, wt_list, idf, UB]
        valuestr = simplejson.dumps(new_list, ensure_ascii=False)
        client.set(savestr, valuestr)


def brand_write_to_redis(client, term_ii, term_wt, term_idf, term_UB):
    sufix = '_brand'
    for term in term_idf:
        savestr = term + sufix
        inverted_index_list = term_ii[term]
        wt_list = term_wt[term]
        idf = term_idf[term]
        UB = term_UB[term]
        new_list = [inverted_index_list, wt_list, idf, UB]
        valuestr = simplejson.dumps(new_list, ensure_ascii=False)
        client.set(savestr, valuestr)

def add_to_term_inverted_index(term_dict, term, did):
    if did is None:
        return
    try:
        posting_list = term_dict[term]
    except KeyError:
        term_dict[term] =[]
        posting_list = term_dict[term]
    if did not in posting_list:
        posting_list.append(did)

def add_to_did_count_dict(did_dict, term, did):
    if did is None:
        return
    try:
        did_count_dict = did_dict[did]
        did_count_dict_totalcount = did_count_dict['_totalcount']

    except KeyError:
        did_dict[did] = {}
        did_count_dict = did_dict[did]
        did_count_dict['_totalcount'] = 0
        did_count_dict_totalcount = did_count_dict['_totalcount']

    try:
        term_count = did_count_dict[term]
    except KeyError:
        did_count_dict[term] = 0
        term_count = did_count_dict[term]
    
    term_count = term_count+1
    did_count_dict_totalcount = did_count_dict_totalcount+1
    did_count_dict[term] = term_count
    did_count_dict['_totalcount'] = did_count_dict_totalcount

def add_a_num_before(first_num, num_to_add):
    first_str=str(first_num)
    num_str = str(num_to_add)
    new_num = int(first_str+num_str)
    return new_num


def read_features_from_txt(filename):
    shop_term_inverted_index={}
    shop_did_feature_count = {}

    brand_term_inverted_index={}
    brand_did_feature_count = {}

    f_feature = open(filename, 'r')
    for line in f_feature:
        json_feature= simplejson.loads(line)
        try:
            brand_id = json_feature['brand_id']
        except KeyError:
            brand_id =0
        try:
            shop_id = json_feature['shop_id']
        except KeyError:
            shop_id = 0
        classification = json_feature['classification']
        tags = json_feature['tags']
        
        if brand_id != 0:
            brand_did = add_a_num_before(2, brand_id)
        else:
            brand_did = None

        if shop_id != 0:
            shop_did = add_a_num_before(1, shop_id)
        else:
            shop_did = None

        for tag in tags:
            if tag =='':
                continue

            add_to_term_inverted_index(shop_term_inverted_index, tag, shop_did)
            add_to_term_inverted_index(brand_term_inverted_index, tag, brand_did)
            add_to_did_count_dict(shop_did_feature_count, tag, shop_did)
            add_to_did_count_dict(brand_did_feature_count, tag, brand_did)

        for type in classification:
            if type == '':
                continue

            add_to_term_inverted_index(shop_term_inverted_index, type, shop_did)
            add_to_term_inverted_index(brand_term_inverted_index, type, brand_did)
            add_to_did_count_dict(shop_did_feature_count, type, shop_did)
            add_to_did_count_dict(brand_did_feature_count, type, brand_did)

    return (shop_term_inverted_index, brand_term_inverted_index, shop_did_feature_count, brand_did_feature_count)

def sort_inverted_index(term_ii):
    for term in term_ii:
        list = term_ii[term]
        list.sort()

def shop_get_term_did_wt(term_ii, did_feature_count):
    terms_wt = {}
    for term in term_ii:
        terms_wt[term] = []
        term_did_wt_list = terms_wt[term]
        did_list = term_ii[term]
        for did in did_list:
            did_count_dict = did_feature_count[did]
            lenth = did_count_dict['_totalcount']
            lenth = math.log(lenth)+10
            did_wt = float(did_count_dict[term])/lenth
            term_did_wt_list.append(did_wt)
    return terms_wt

def brand_get_term_did_wt(term_ii, did_feature_count):
    terms_wt = {}
    for term in term_ii:
        terms_wt[term] = []
        term_did_wt_list = terms_wt[term]
        did_list = term_ii[term]
        for did in did_list:
            did_count_dict = did_feature_count[did]
            did_wt = float(did_count_dict[term])/did_count_dict['_totalcount']
            term_did_wt_list.append(did_wt)
    return terms_wt

def get_term_UB_term_idf(terms_wt, did_feature_count):
    total_did = len(did_feature_count)
    UB = {}
    terms_idf ={}
    for term in terms_wt:
        term_posting = terms_wt[term]
        idf = float(total_did)/len(term_posting)
        idf = math.log(idf)
        wt_max = max(term_posting)
        UB[term] = wt_max*idf
        terms_idf[term] = idf

    return (UB, terms_idf)

def save_to_json_txt(dict, filename):
    f_txt = open(filename, 'w')
    for term in dict:
        new_dict ={}
        new_dict[term] = dict[term]
        str = simplejson.dumps(new_dict, ensure_ascii=False)
        f_txt.write(str)
        f_txt.write('\n')

def read_to_dict(filename):
    f_txt = open(filename, 'r')
    dict = {}
    for str in f_txt:
        new_dict = simplejson.loads(str)
        for term in new_dict:
            dict[term] = new_dict[term]

    return dict

if __name__ == '__main__':
    #term_ii, did_feature_count = read_features_from_txt('goods_features_test.txt')
    shop_term_ii, brand_term_ii, shop_did_feature_count, brand_did_feature_count = read_features_from_txt('/data_ssd/work_data/liqin/item_model/origin/goods_features.txt')
    sort_inverted_index(shop_term_ii) 
    sort_inverted_index(brand_term_ii) 
    shop_terms_wt = shop_get_term_did_wt(shop_term_ii, shop_did_feature_count)
    shop_UB, shop_terms_idf = get_term_UB_term_idf(shop_terms_wt, shop_did_feature_count)

    brand_terms_wt = brand_get_term_did_wt(brand_term_ii, brand_did_feature_count)
    brand_UB, brand_terms_idf = get_term_UB_term_idf(brand_terms_wt, brand_did_feature_count)


    redisclient = connect_redis()
    shop_write_to_redis(redisclient, shop_term_ii, shop_terms_wt, shop_terms_idf, shop_UB)
    brand_write_to_redis(redisclient, brand_term_ii, brand_terms_wt, brand_terms_idf, brand_UB)

    #save_to_json_txt(shop_term_ii, './weak_and_data/shop_term_inverted_index.txt') 
    #save_to_json_txt(shop_terms_wt, './weak_and_data/shop_terms_wt.txt')
    #save_to_json_txt(shop_terms_idf, './weak_and_data/shop_terms_idf.txt')
    #save_to_json_txt(shop_UB, './weak_and_data/shop_UB.txt')
    #save_to_json_txt(shop_did_feature_count, './weak_and_data/shop_did_feature_count.txt')

    #save_to_json_txt(brand_term_ii, './weak_and_data/brand_term_inverted_index.txt') 
    #save_to_json_txt(brand_terms_wt, './weak_and_data/brand_terms_wt.txt')
    #save_to_json_txt(brand_terms_idf, './weak_and_data/brand_terms_idf.txt')
    #save_to_json_txt(brand_UB, './weak_and_data/brand_UB.txt')
    #save_to_json_txt(brand_did_feature_count, './weak_and_data/brand_did_feature_count.txt')


    #term_ii_read = read_to_dict('shop_term_inverted_index.txt')
    #terms_wt_read = read_to_dict('shop_terms_wt.txt')
    #terms_idf_read = read_to_dict('shop_terms_idf.txt')
    #UB_read = read_to_dict('shop_UB.txt')
    

    
#    for term in term_ii_read:
 #       print UB_read[term], terms_idf_read[term]
  #      print term, term_ii_read[term], terms_wt_read[term]
   #     print '\n\n\n'

#    print '\n\n\n\n'
 #   for did in did_feature_count:
  #      print did, ':'
   #     did_count_dict = did_feature_count[did]
    #    for term in did_count_dict:
     #       print term, ' count: ', did_count_dict[term]
