# SQL solution tested with Spark
library(sparklyr)
library(tidyverse)
library(data.table)
library(DBI)

# Simple ETL to solve two issues:
# 1. transform Visit_Time to timestamp due to absence of time data type for Spark
# 2. There is no duplicated pageview in original dataset which is a testcase we want to add

# Extract
# sample_pageview <- read_csv("input/SamplePageviews.csv")
# 
# # Transform
# sample_pageview <- sample_pageview %>% 
#   union(data_frame(ID=13L,User_ID=1L,Page_ID=54L, Visit_Date=as.Date("2018-01-01"), Visit_Time=hms::as.hms("11:56:12"))) %>%
#   mutate(
#     Visit_Time = lubridate::ymd_hms(str_c(Visit_Date, Visit_Time, sep = " "))) 
# # Saved as CSV for future use
# write_csv(sample_pageview,  "input/SamplePageviews_transformed.csv")

# Load
sc <- spark_connect(master = "local")
sample_pageview <- read_csv("input/SamplePageviews_transformed.csv", 
                            col_types = cols(Visit_Date = col_character()))
copy_to(sc, sample_pageview, overwrite = TRUE)

# Execute SQL Query
dbGetQuery(sc, read_file("unique_pageview.sql")) %>%
  write_csv("output/unique_pageview.csv")

# R Solution for reference
# 
# library(tidyverse)
# library(data.table)
# library(lubridate)
# # Determine new session
# sample_pageview <- fread("input/SamplePageviews_transformed.csv")
# setorder(sample_pageview, User_ID, Visit_Date, Visit_Time)
# sample_pageview[,Visit_Time:=ymd_hms(Visit_Time)]
# sample_pageview[,time_since_last_activity:=difftime(Visit_Time, shift(Visit_Time), units="secs"), by=.(User_ID, Visit_Date)]
# sample_pageview[,is_new_session:=as.integer(is.na(time_since_last_activity)|time_since_last_activity>10*60)]
# sample_pageview[, session_id := cumsum(is_new_session)]
# # Compute Result
# unique_pageview <- sample_pageview[,.(Total_User_Sessions = length(unique(session_id))), by=.(Page_ID, Visit_Date)]
# setorder(unique_pageview, Page_ID, Visit_Date)
# unique_pageview %>% write_csv("output/unique_pageview.csv")
