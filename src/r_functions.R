##### Function Load Libraries #####
function_load_libraries <- function()
{
  packages <- c(
    #Data Transformation
    'tidyverse', 'janitor', 'glue', 'lubridate', 'data.table',

    #Connection
    'DBI', 'odbc',

    #Loading
    'readxl', 'readr', 'httr',

    #Baseball
    'MLBaseballR'

  )
  install_packages <- packages[!(packages %in% installed.packages()[,'Package'])]
  if(length(install_packages))
    install.packages(install_packages, repos = 'http://cran.rstudio.com', dependencies = TRUE)
  sapply(packages, require, character.only = TRUE)
}


##### ODBC Connection #####
function_odbc_set_up <- function() {

  connection <- "sqlserverpc"

  odbc_connection_pc <<- dbConnect(odbc::odbc(), connection, timeout = 60)

}


##### Function Sleep #####
function_sleep <- function(sleep_from = 5, sleep_to = 10){
  wait <- runif(1, sleep_from, sleep_to)
  print(paste('I am about to wait for', wait,'seconds', sep = " "))
  print(paste('I will resume request at', Sys.time()+wait, sep = ' '))
  Sys.sleep(wait)
}


##### Logging Functions #####
function_logging <- function(){
  end_time <- Sys.time()
  cat(
    paste('I am back to r_master', Sys.time(), sep = ' '),
    file = log_file,
    append = T,
    sep = '\n'
  )


  ##### Upload Operations
    odbc_connection_pc <- dbConnect(
      odbc(),
      dsn = 'sqlserverpc')

  #Create temp data
  temp_data_master <- data.frame(
    operations_number = operations_number,
    operations_name = operations,
    start_time = start_time,
    end_time = end_time
  )

  ##### Load Latest Data to MySQL
  dbWriteTable(
    odbc_connection_pc,
    value = temp_data_master,
    name = 'master_time_tracking',
    append = TRUE#,
    #filed.type = class_type
  )

  cat(
    paste('I am done with uploading master_time_tracking', Sys.time(), sep = ' '),
    file = log_file,
    append = T,
    sep = '\n'
  )

}

##### Mapping short team names to MLB team_id

helper_team_id <- function(t) {

  dplyr::case_when(
    stringr::str_detect(t,'angels') ~ 108,
    stringr::str_detect(t,'dbacks') ~ 109,
    stringr::str_detect(t,'orioles') ~ 110,
    stringr::str_detect(t,'red sox') ~ 111,
    stringr::str_detect(t,'cubs') ~ 112,
    stringr::str_detect(t,'reds') ~ 113,
    stringr::str_detect(t,'indians') ~ 114,
    stringr::str_detect(t,'guardians') ~ 114,
    stringr::str_detect(t,'rockies') ~ 115,
    stringr::str_detect(t,'tigers') ~ 116,
    stringr::str_detect(t,'astros') ~ 117,
    stringr::str_detect(t,'royals') ~ 118,
    stringr::str_detect(t,'dodgers') ~ 119,
    stringr::str_detect(t,'nationals') ~ 120,
    stringr::str_detect(t,'mets') ~ 121,
    stringr::str_detect(t,'athletics') ~ 133,
    stringr::str_detect(t,'pirates') ~ 134,
    stringr::str_detect(t,'padres') ~ 135,
    stringr::str_detect(t,'mariners') ~ 136,
    stringr::str_detect(t,'giants') ~ 137,
    stringr::str_detect(t,'cardinals') ~ 138,
    stringr::str_detect(t,'rays') ~ 139,
    stringr::str_detect(t,'rangers') ~ 140,
    stringr::str_detect(t,'blue jays') ~ 141,
    stringr::str_detect(t,'twins') ~ 142,
    stringr::str_detect(t,'phillies') ~ 143,
    stringr::str_detect(t,'braves') ~ 144,
    stringr::str_detect(t,'white sox') ~ 145,
    stringr::str_detect(t,'marlins') ~ 146,
    stringr::str_detect(t,'yankees') ~ 147,
    stringr::str_detect(t,'brewers') ~ 158
  )

}
