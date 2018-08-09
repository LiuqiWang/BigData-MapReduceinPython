# -*- coding: utf-8 -*-
"""
Created on Tue Feb 06 16:10:23 2018

@author: liuqi
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRtest(MRJob):    
    def steps(self):
        return[
            MRStep(mapper=self.mapper_first,
                   reducer=self.reducer_first),
            MRStep(mapper=self.mapper_again,
                   reducer=self.sorting)
        ]
        
    def mapper_first(self, key, line):
        (customerID, itemID, AmountSpent)=line.split(',') #split line by line, its just a name. Becasue of the original class, Python will read the document line by line by defaul. 
        yield customerID, float(AmountSpent) # will order the key in ascending order. 
    
    def reducer_first(self, customerID, AmountSpent):
        yield customerID, sum(AmountSpent)
    
    def mapper_again(self, customerID, sumAmount): # this mapper reads from the reducer
        yield '%04.02f'%float(sumAmount), customerID
    
    def sorting(self, sumAmount, customerIDs):
        for customerID in customerIDs:
            yield customerID, sumAmount

if __name__ == '__main__':
    MRtest.run()
    
    
#!python SpentByCustomerOrder.py DataA1.csv>SpentByCustomerOrder.txt    
    
    
    
    
    
    
    