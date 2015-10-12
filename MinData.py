#0 -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 12:05:41 2015

@author: Askery Canabarro & Jhonny Everson

THIS CODE IS TO COMPUTE MINUTE DATA FROM STOCK MARKET (BRAZILIAN STOCK LAYOUT ATTACHED)
"""

from pyspark import SparkContext
#1 ALL DAILY FILES ARE IN THE SAME FOLDER - JUST ADDRESSING THE FOLDER PATH IS ENOUGHT TO GATHER EVERYTHING
logFile = "/media/data/rowData_201304_201508/NEG_20130920.TXT"  # Should be some file on your system
sc = SparkContext("local", "Minute Data")
logData = sc.textFile(logFile)

#2 SELECTION OF STOCKS 
stocks = ['PETR3', 'PETR4' ,'ABEV3', 'BBAS3', 'BBDC4', 'BVMF3', 'GGBR4', 'ITSA4', 'ITUB4','VALE3', 'VALE5']
def filterStock(line):
    return any(keyword in line for keyword in stocks)

#3 IF WANT ALL STOCKS, COMMENT EVERYTHING BETWEEN #2 AND #3 AND UNCOMMENT EVERYTHING BETWEEN #3 AND #4
# CODE TO ALL SOON

#4 SELECTION OF SUITABLE COLUMNS AND KEY
def makeColumns(line):
    pieces = line.split(';');   # SEPARATORS ARE ";"
    date   = pieces[0]          # FIRST COLUMN  - DATE
    symb   = pieces [1].strip() # SECOND COLUMN - STOCK SYMBOL
    hour   = pieces[5][:5]      # TRASANCTION TIME IN FORMAT HH:MM
    key    = symb + date + hour # KEY DEFINITION AS CONCATENATION OF TIME COLUMNS
    output = str("%.2f" % float(pieces[3])) + "   " + str(int(pieces[4])) # OUTOUT FORMAT WITHOUT 'U', ',' OR '()'
    return (key, (symb, (output) ))

#5 SELECT FIRST MINUTE OCCURENCY, TO PICK LAST USE 'return b;'
def minData(a, b):
    return a;

#6 CREATE A RDD OF THE SELECT VALUES FOR COLUMNS (makeColumns) AND ROWS(minData)
result = logData.filter(filterStock).map(makeColumns).reduceByKey(minData).sortByKey()

#7 OUTPUT IN TEXT FILE: ".coalesce(1)" PUT EVERYTHING IN JUST ONE OUTPUT FILE
result.coalesce(1).saveAsTextFile('teste')
