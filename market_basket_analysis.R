# Market Basket Analysis
# Reference: https://towardsdatascience.com/a-gentle-introduction-on-market-basket-analysis-association-rules-fa4b986a40ce

# SQL solution tested with Spark local cluster
library(sparklyr)
library(tidyverse)
library(DBI)
sc <- spark_connect(master = "local")
sample_orders <- read_csv("input/SampleOrders.csv")
copy_to(sc, sample_orders, overwrite = TRUE)
dbGetQuery(sc, read_file("market_basket_analysis.sql"))



# R Solution for reference
# library(data.table)
# library(arules)
# 
# orders <- fread("input/SampleOrders.csv")
# # Make sure OrderID x ProductID is unique
# orders <- orders[,.(Quantity=sum(Quantity)), by=.(OrderID, ProductID)]
# 
# # Compute bestseller
# bestseller <- orders[,.(count=.N), by=.(ProductID)]
# setorder(bestseller, -count)
# # There are 7 products with 14 purchases, for simplicity, we randomly pick 3 as the top 8th, 9th,10th
# bestseller <- head(bestseller, 10)
# 
# # Compute supp, conf, lift
# trans <- as(split(factor(orders[["ProductID"]]), orders[["OrderID"]]), "transactions")
# rules <- apriori(trans, parameter = list(supp = 0.0002, conf = 0.4, minlen=2,target = "rules"))
# inspect(head(rules, by = "lift"))