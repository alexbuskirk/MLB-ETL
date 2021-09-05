## For Task Scheduler  ----
setwd('C:/Users/catspur/Desktop/projects/MLB-ETL')

## Load Libraries & Functions  ----

source('src/r_functions.R')

function_load_libraries()

## Daily Operations ----

source('src/r_update_schedule.R')
