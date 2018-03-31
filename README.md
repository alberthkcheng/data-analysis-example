# Data Analysis Example
This repository contains examples of frequently used SQL script for data analysis.

### Part I: Finding Unique Pageviews 
Given pageview activity for an user, find the total number of sessions each page per day. (a.k.a. Unique Pageviews for Google Analytics)

[Original Dataset](input/SamplePageviews.csv) | [Transformed Dataset](input/SamplePageviews_transformed.csv) | **[SQL Solution](unique_pageview.sql)** | [R script for ETL and setting up testing environment](unique_pageview.R)

***\* An extra entry is added as a test case for removing duplicated pageview by users***  
***\* A simple ETL is performed to turn Visit_Time into Datetime. See [Testing](#how-to-test--) section for details.***

### Part II: Market Basket Analysis
Given transaction data, find a list of products frequently purchased with the top 10 bestseller and compute association rules measurement Support, Confidence, Lift Ratio. I read [this article](https://towardsdatascience.com/a-gentle-introduction-on-market-basket-analysis-association-rules-fa4b986a40ce) few weeks ago which can be taken as reference.

[Sample Dataset](input/SampleOrders.csv) | **[SQL Solution](market_basket_analysis.sql)** | [R script for ETL and setting up testing environment](market_basket_analysis.R)

*\* One simple use case I did before is to find travel destination cluster for Hong Kong people, for example, Germany usually comes with Austria, and some Eastern Europe also cluster together.*  
*\* No rules has support >= 0.2, another set of parameter is prepared for testing purpose*  

### How to test - 

When considering how to test the SQL script locally, I finally decided to set up a **Spark** local cluster through **R** interface  based on the following:  
- SQLite doesn't support windows function  
- There are too many setting when setting up a Postgresql database  

One drawback for Spark is that it doesn't support time datatype. So in part 1 I transformed the time column into datetime.

**To test:**  
- Open RStudio  
- Install the required R packages (sparklyr, tidyverse, DBI)  
- Install spark using `spark_install(version = "2.1.0")`  
- Run the R scripts provided