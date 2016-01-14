#!/usr/bin/env python
# coding=utf-8

import sys 
reload(sys)
sys.setdefaultencoding('utf8')

import time
import heapq
import simplejson
import redis
from DBsetting import *
from Weak_And_Settings import *

#UB = {"t0":0.5,"t1":1,"t2":2,"t3":3,"t4":4} #upper bound of term's value
#MAX_RESULT_NUM = 3 #max result number 

def connect_redis():
    redisClient=redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    return redisClient

class Term_Data:
    '''该类返回所需要的倒排索引，wt，idf字典'''
    def __init__(self):
        self.client = connect_redis()

    def get_query_dict(self, query_terms, search_kind = 'shop'):
        if len(query_terms) == 0:
            return []
        sufix = '_'+search_kind
        term_inverted_index = {} 
        term_wt = {}
        term_idf = {}
        term_UB = {}
        for term in query_terms:
            keystr = term+sufix
            term_value = self.client.get(keystr)
            if term_value == None:
                continue

            term_data_list = simplejson.loads(term_value)
            term_inverted_index[term] = term_data_list[0]
            term_wt[term] = term_data_list[1]
            term_idf[term] = term_data_list[2]
            term_UB[term] = term_data_list[3]

        return_list = [term_inverted_index, term_wt, term_idf, term_UB]
        return return_list





class WAND:
    #initial index
    def __init__(self, term_data, kind = 'shop'):
        self.search_kind = kind
        self.term_data = term_data
        self.result_list = [] #result list
        self.invert_index =  {} #self.read_to_dict(invert_index_file) #InvertIndex: term -> docid1, docid2, docid3 ...
        self.UB = {}  #self.read_to_dict(UB_file)
        self.terms_wt =  {}  #self.read_to_dict(terms_wt_file)
        self.terms_idf =  {} # self.read_to_dict(terms_idf_file)
        self.current_doc = 0
        self.current_invert_index = {} #posting
        self.query_terms = []
        self.threshold = -1
        self.sort_terms = []
        self.LastID = 2000000000 #big num
        self.last_docid = 2000000000#last_docid
        #倒排索引添加最后一个id需要放到别的地方完成
        #self.add_lastID_to_dict(self.invert_index, self.LastID)
    
    def set_query_dict(self, query_terms):
        #clear first
        self.invert_index = {}
        self.UB = {}
        self.terms_idf = {}
        self.terms_wt = {}
        query_dict_list = self.term_data.get_query_dict(query_terms, self.search_kind)
        if len(query_dict_list) == 0:
            return

        self.invert_index = query_dict_list[0]
        self.terms_wt = query_dict_list[1]
        self.terms_idf = query_dict_list[2]
        self.UB = query_dict_list[3]
        self.add_lastID_to_dict(self.invert_index, self.LastID)



    def read_to_dict(self, filename):
        f_txt = open(filename, 'r')
        dict = {}
        for str in f_txt:
            new_dict = simplejson.loads(str)
            for term in new_dict:
                #if term == '足球鞋':
                 #   print term, type(term)
                dict[term] = new_dict[term]

        return dict

    def add_lastID_to_dict(self, dict, id):
        for term in dict:
            posting_list = dict[term]
            posting_list.append(id)


    #get index list according to query term
    def __InitQuery(self, query_terms):
        self.current_doc = -1
        self.current_invert_index.clear()
        self.query_terms = []
        self.sort_terms[:] = []
        self.threshold = -1
        self.result_list = [] #result list
        
        self.set_query_dict(query_terms)
        for term in query_terms:
            #initial start pos from the first position of term's invert_index
            try:
                self.current_invert_index[term] = [ self.invert_index[term][0], 0 ] #[ docid, index ]
                self.query_terms.append(term)
            except KeyError:
                continue

    
    #sort term according its current posting doc id
    def __SortTerms(self):
        if len(self.sort_terms) == 0:
            for term in self.query_terms:
                if term in self.current_invert_index:
                    doc_id = self.current_invert_index[term][0]
                    self.sort_terms.append([ int(doc_id), term ])
        self.sort_terms.sort()

    #select the first term in sorted term list
    def __PickTerm(self, pivot_index):
        return 0

    #find pivot term
    def __FindPivotTerm(self):
        score = 0
        #print "sort term ", self.sort_terms  #[docid, term]
        for i in range(0, len(self.sort_terms)):
            score = score + self.UB[self.sort_terms[i][1]]
            if score >= self.threshold:
                return [ self.sort_terms[i][1], i] #[term, index]

        return [ None, len(self.sort_terms)]

    #move to doc id >= docid
    def __IteratorInvertIndex(self, change_term, docid, pos):
        doc_list = self.invert_index[change_term]
        i = 0
        for i in range(pos, len(doc_list)):
            if doc_list[i] >= docid:
                pos = i
                docid = doc_list[i]
                break

        return [ docid, pos ]

    
    def __AdvanceTerm(self, change_index, docid ):
        change_term = self.sort_terms[change_index][1]
        pos = self.current_invert_index[change_term][1]
        (new_doc, new_pos) = self.__IteratorInvertIndex(change_term, docid, pos)
        
        self.current_invert_index[change_term] = [ new_doc , new_pos ]
        self.sort_terms[change_index][0] = new_doc

    def __Next(self):
        if self.last_docid == self.current_doc:
            return None
        
        while True:
            #sort terms by doc id
            self.__SortTerms()
            
            #find pivot term > threshold
            (pivot_term, pivot_index) = self.__FindPivotTerm()
            if pivot_term == None:
                #no more candidate
                return None
            
            pivot_doc_id = self.current_invert_index[pivot_term][0]
            
            if pivot_doc_id == self.LastID: #!!
                return None
            
            if pivot_doc_id <= self.current_doc:
                change_index = self.__PickTerm(pivot_index)#always retrun 0
                self.__AdvanceTerm( change_index, self.current_doc + 1 )
            else:
                first_docid = self.sort_terms[0][0]
                if pivot_doc_id == first_docid:
                    self.current_doc = pivot_doc_id
                    return self.current_doc
                else:
                    #pick all preceding term,advance to pivot
                    for i in range(0, pivot_index):
                        change_index = i
                        self.__AdvanceTerm( change_index, pivot_doc_id )

    def __InsertHeap(self,doc_id,score):
        if score == 0.0:
            if len(self.result_list):
                return self.result_list[0][0]
            else:
                return -1

        if len(self.result_list)<MAX_RESULT_NUM:
            heapq.heappush(self.result_list, (score, doc_id))
        else:
            if score>self.result_list[0][0]: #large than mini item in heap
                heapq.heappop(self.result_list)
                heapq.heappush(self.result_list, (score, doc_id))
        return self.result_list[0][0]

    #full evaluate the doucment, get its full score, to be added
    def __FullEvaluate(self, docid):
        Sum = 1
        for term in self.query_terms:
            term_postinglist = self.invert_index[term]
            try:
                index = term_postinglist.index(docid)
                Sum = Sum * self.terms_wt[term][index] * self.terms_idf[term]
            except ValueError:
                return 0.0

        return Sum


    def DoQuery(self, query_terms):
        self.__InitQuery(query_terms)
        print 'query_terms:'
        for term in self.query_terms:
            print term
        if len(self.query_terms) == 0:
            return None
        while True:
            candidate_docid = self.__Next()
            if candidate_docid == None:
                break
            #print "candidate_docid:" + str(candidate_docid)
            #insert candidate_docid to heap
            full_doc_score = self.__FullEvaluate(candidate_docid)
            mini_item_value = self.__InsertHeap(candidate_docid, full_doc_score)
            #update threshold
            self.threshold = mini_item_value
            #print "result list ", self.result_list
        return self.result_list

def print_result(result):
    if result == None:
        print 'no result\n'
        return 
    print "final result "
    for item in final_result:
        print "doc " + str(item[1])
    print '\n\n\n'



if __name__ == "__main__":
   # testIndex = {}
   # testIndex["t0"] = [ 1, 3, 26, 2000000000]
    #testIndex["t1"] = [ 1, 2, 4, 10, 100, 2000000000 ]
    #testIndex["t2"] = [ 2, 3, 6, 34, 56, 2000000000 ]
    #testIndex["t3"] = [ 1, 4, 5, 23, 70, 200, 2000000000 ]
    #testIndex["t4"] = [ 5, 14, 78, 2000000000 ]
    
    
    term_data = Term_Data()

    w_shop = WAND(term_data, 'shop')
    #w_shop = WAND(SHOP_TERM_INVERTED_INDEX_FILE, SHOP_UB_FILE, SHOP_TERMS_WT_FILE, SHOP_TERMS_IDF_FILE)
    

    #final_result = w.DoQuery([u'haha',u'足球鞋', u'男装', u'跑步鞋', u'三叶草'])

    w_brand = WAND(term_data, 'brand')
    #w_brand = WAND(BRAND_TERM_INVERTED_INDEX_FILE, BRAND_UB_FILE, BRAND_TERMS_WT_FILE, BRAND_TERMS_IDF_FILE)

    print 'shop search result:\n'

    final_result = w_shop.DoQuery([u'戒指', u'情侣', u'韩式']) #1197393, 1294762, 1168453
    print_result(final_result)


    final_result = w_shop.DoQuery([u'羊毛呢外套', u'雪纺',  u'翻领', u'分裤'])  #16674
    print_result(final_result)


    final_result = w_shop.DoQuery([u'杰朴森', u'提携',  u'莫霍克', u'金岛'])  #None
    print_result(final_result)


    final_result = w_shop.DoQuery([ u'提携'])  #1198906
    print_result(final_result)


    final_result = w_shop.DoQuery([ u'123', '456'])  #1198906
    print_result(final_result)

    final_result = w_shop.DoQuery([])  #None
    print_result(final_result)

    print 'brand search result:\n'
    final_result = w_brand.DoQuery([u'撞色', u'外套' ]) #22908
    print_result(final_result)


    
    final_result = w_brand.DoQuery([u'松子', u'坚果',  u'开口'])  #237529
    print_result(final_result)


    final_result = w_brand.DoQuery([ u'双卡双待', u'小女孩'])  #None
    print_result(final_result)

    final_result = w_brand.DoQuery([ u'双卡双待'])  #2389, 21609
    print_result(final_result)

    final_result = w_brand.DoQuery([ u'hahaha', u'just do it'])  #None
    print_result(final_result)

    final_result = w_brand.DoQuery([])  #None
    print_result(final_result)
