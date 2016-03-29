# StockMinData
PySpark Code for Minute Data from Stock Exchange


We want the code to extract just one example within the same minute: the first or the last, for instance.
In this example:

STOCK   TRADE TIME      PRICE

XXX     10:33:645       18.54
XXX     10:34:123       18.65
XXX     10:34:234       18.62
YYY     10:07:345       45.74

It must return (taking the first occurency):

XXX     10:33:645       18.54
XXX     10:34:123       18.65
YYY     10:07:345       45.74


PERFORMANCE OBSERVATIONS

1) It took around 4 hours and 30 minutes to process 132GB of data (683 trading days) and extract minute data of the 11 most traded brazilian stock 
in a ordinary laptop (Quad Core intel i5 CPU @ 2.50 GHz with 8GB system memory.)

2) Without using Spark we get a crash memory error as expected with so many data.


