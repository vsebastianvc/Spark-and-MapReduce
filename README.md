# InMapper combiner technique application to solve aggregation of high-volume data problem in telecommunication company

# Prerequisites
Be sure that you already has installed the next programs/enviroments in your machine

### Spark 
### Scala
### Hadoop
### MapReduce


# Context of Project 

R Factor:
---------
R-Factor is a report that any Telecommunication company has to deliver to regulatory entity 
(CRC) each 3 months in Colombia. This report has to illustrate the QoS (Quality of Voice) of 
the calls in average of the last tree months, by City. The measure to report must be R-Factor,
so the report inherits that name.

Background:
-----------
To built this report the company had to invest more than 3 months of 3 persons to develop it:
1. To develop the ETL phase, using MS Integration services to extract each 15 minutes the 
detailed calls from a mySQL Database. This database upload the files from 5 different 
Telecommunication Platforms called Session Border Controllers that has this information in
flat files.
2. To implement a datawarehouse using MS SQL Server to store this high volume of detailed 
calls, with fixed structure (tables).
3. To develop a cube in MS Analysis Services and a process in Transact-SQL to aggregate the
detailed calls and obtain the average of R-Factor to the City level.

The challenge:
--------------
To develop again the R-Factor report using the Map Reduce potential, using a real sample file.

Conclusion:
-----------
Use of Map Reduce is optimal in this case: All this was made for a single Person in 1:30 hours
using only map-reduce and would avoid to have the data trice: flat files+disk in mySQL DB+disk
in datawarehouse in SQL Server (only original flat files would be neccesary as input to
Map Reduce) and in a faster way (avoid the chain of processing).



# Authors
### Carlos Hernandez
### Sebastian Valencia
