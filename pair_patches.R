# pairs maps to patch data using start times
source('db_info.R')
require(tidyverse)
require(lubridate)
require(RMySQL)
require(fuzzyjoin)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

dbDisconnect(mydb)