# R Solution to find the number of unique pageviews per day
# library(tidyverse)
# library(data.table)
# 
# sample_pageview <- fread("input/SamplePageviews.csv")
# setorder(sample_pageview, User_ID, Visit_Date, Visit_Time)
# sample_pageview[,Visit_Time:=hms::parse_hms(Visit_Time)]
# 
# # Determine new session
# sample_pageview[,time_since_last_activity:=Visit_Time-shift(Visit_Time), by=.(User_ID, Visit_Date)]
# sample_pageview[,is_new_session:=as.integer(is.na(time_since_last_activity)|time_since_last_activity>10*60)]
# sample_pageview[, session_id := cumsum(is_new_session)]
# # Compute Result
# unique_pageview <- sample_pageview[,.(Total_User_Sessions = length(unique(session_id))), by=.(Page_ID, Visit_Date)]
# setorder(unique_pageview, Page_ID, Visit_Date)
# 
# unique_pageview %>% write_csv("output/unique_pageview.csv")


# SQL solution tested with Spark
library(sparklyr)
library(tidyverse)
library(DBI)
sc <- spark_connect(master = "local")
sample_pageview <- read_csv("input/SamplePageviews.csv", col_types = cols(.default = col_character()))
sample_pageview <- sample_pageview %>% 
  mutate(
    Visit_Time = lubridate::ymd_hms(str_c(Visit_Date, Visit_Time, sep = " ")))
sample_pageview <- copy_to(sc, sample_pageview, overwrite = TRUE)
dbGetQuery(sc, read_file("unique_pageview.sql"))

# Problem - there is no duplicated pageview - have to add a test case