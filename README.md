# Data Analysis Example
This repository contains examples of frequently used SQL script for data analysis.

### Part I: Finding Unique Pageviews 
Given pageview activity for an user, find the total number of sessions each page per day. (a.k.a. Unique Pageviews for Google Analytics)

[Original Dataset](input/SamplePageviews.csv) | [Transformed Dataset](input/SamplePageviews_transformed.csv) | **[SQL Solution](unique_pageview.sql)** | [R script for ETL and setting up testing environment](unique_pageview.R)

***\* An extra entry is added as a test case for removing duplicated pageview by users***  
***\* A simple ETL is performed to turn Visit_Time into Datetime. See [Testing](#how-to-test--) section for details.***

### Part II: Market Basket Analysis


### How to test - 

When considering how to test the SQL script locally, I finally decided to set up a **Spark** local cluster through **R** interface  based on the following:  
- SQLite doesn't support windows function  
- There are too many setting when setting up a Postgresql database  

One drawback for Spark is that it doesn't support time datatype. So in part 1 I transformed the time column into datetime.

**To test:**  
- Open R Studio  
- Install the required R packages (sparklyr, tidyverse, DBI)  
- Install spark using `spark_install(version = "2.1.0")`  
- Run the R scripts provided