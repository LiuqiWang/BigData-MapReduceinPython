# -*- coding: utf-8 -*-
"""
Created on Tue Feb 06 15:23:51 2018

@author: liuqi
"""
from mrjob.job import MRJob
    
class MRSpentByCustomer(MRJob):
    def mapper(self, key, line):
        (customerID, itemID, AmountSpent) = line.split(',')
        yield customerID, float(AmountSpent)
    
    def reducer(self, customerID, AmountSpent):
        yield customerID, sum(AmountSpent)
        
if __name__ == '__main__':
    MRSpentByCustomer.run()
        
    
#!python SpentByCustomer.py DataA1.csv>SpentByCustomer.txt