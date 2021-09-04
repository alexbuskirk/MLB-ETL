##### Function Load Libraries####
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

  odbc_connection_pc <<- dbConnect(odbc::odbc(), connection,  database = 'mlb', timeout = 60)

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
  operation_end_dtm <- Sys.time()
  cat(
    paste('back to r_master', Sys.time(), sep = ' '),
    file = log_file,
    append = T,
    sep = '\n'
  )

  #Create temp data
  temp_data_master <- data.frame(
    operation_id = operation_id,
    operation_name = operation_name,
    operation_start_dtm = operation_start_dtm,
    operation_end_dtm = operation_end_dtm
  )

  ##### Load Latest Data to MySQL
  dbWriteTable(
    odbc_connection_pc,
    value = temp_data_master,
    name = 'time_tracking',
    append = TRUE#,
    #filed.type = class_type
  )

  cat(
    paste('done uploading time_tracking', Sys.time(), sep = ' '),
    file = log_file,
    append = T,
    sep = '\n'
  )

}

##### Mapping short team names to MLB team_id

helper_team_id <- function(team,type) {

if (type == 'id') {

  dplyr::case_when(
    stringr::str_detect(team,'angels') ~ 108,
    stringr::str_detect(team,'dbacks') ~ 109,
    stringr::str_detect(team,'orioles') ~ 110,
    stringr::str_detect(team,'red sox') ~ 111,
    stringr::str_detect(team,'cubs') ~ 112,
    stringr::str_detect(team,'reds') ~ 113,
    stringr::str_detect(team,'indians') ~ 114,
    stringr::str_detect(team,'guardians') ~ 114,
    stringr::str_detect(team,'rockies') ~ 115,
    stringr::str_detect(team,'tigers') ~ 116,
    stringr::str_detect(team,'astros') ~ 117,
    stringr::str_detect(team,'royals') ~ 118,
    stringr::str_detect(team,'dodgers') ~ 119,
    stringr::str_detect(team,'nationals') ~ 120,
    stringr::str_detect(team,'mets') ~ 121,
    stringr::str_detect(team,'athletics') ~ 133,
    stringr::str_detect(team,'pirates') ~ 134,
    stringr::str_detect(team,'padres') ~ 135,
    stringr::str_detect(team,'mariners') ~ 136,
    stringr::str_detect(team,'giants') ~ 137,
    stringr::str_detect(team,'cardinals') ~ 138,
    stringr::str_detect(team,'rays') ~ 139,
    stringr::str_detect(team,'rangers') ~ 140,
    stringr::str_detect(team,'blue jays') ~ 141,
    stringr::str_detect(team,'twins') ~ 142,
    stringr::str_detect(team,'phillies') ~ 143,
    stringr::str_detect(team,'braves') ~ 144,
    stringr::str_detect(team,'white sox') ~ 145,
    stringr::str_detect(team,'marlins') ~ 146,
    stringr::str_detect(team,'yankees') ~ 147,
    stringr::str_detect(team,'brewers') ~ 158
  )

  } else if (type == 'abbrv') {

  dplyr::case_when(
    stringr::str_detect(team,'angels') ~ 'LAA',
    stringr::str_detect(team,'dbacks') ~ 'ARI',
    stringr::str_detect(team,'orioles') ~ 'BAL',
    stringr::str_detect(team,'red sox') ~ 'BOS',
    stringr::str_detect(team,'cubs') ~ 'CHC',
    stringr::str_detect(team,'reds') ~ 'CIN',
    stringr::str_detect(team,'indians') ~ 'CLE',
    stringr::str_detect(team,'guardians') ~ 'CLE',
    stringr::str_detect(team,'rockies') ~ 'COL',
    stringr::str_detect(team,'tigers') ~ 'DET',
    stringr::str_detect(team,'astros') ~ 'HOU',
    stringr::str_detect(team,'royals') ~ 'KC',
    stringr::str_detect(team,'dodgers') ~ 'LAD',
    stringr::str_detect(team,'nationals') ~ 'WSH',
    stringr::str_detect(team,'mets') ~ 'NYM',
    stringr::str_detect(team,'athletics') ~ 'OAK',
    stringr::str_detect(team,'pirates') ~ 'PIT',
    stringr::str_detect(team,'padres') ~ 'SD',
    stringr::str_detect(team,'mariners') ~ 'SEA',
    stringr::str_detect(team,'giants') ~ 'SF',
    stringr::str_detect(team,'cardinals') ~ 'STL',
    stringr::str_detect(team,'rays') ~ 'TB',
    stringr::str_detect(team,'rangers') ~ 'TEX',
    stringr::str_detect(team,'blue jays') ~ 'TOR',
    stringr::str_detect(team,'twins') ~ 'MIN',
    stringr::str_detect(team,'phillies') ~ 'PHI',
    stringr::str_detect(team,'braves') ~ 'ATL',
    stringr::str_detect(team,'white sox') ~ 'CWS',
    stringr::str_detect(team,'marlins') ~ 'MIA',
    stringr::str_detect(team,'yankees') ~ 'NYY',
    stringr::str_detect(team,'brewers') ~ 'MIL'
  )

  } else if (type == 'bbref') {

    dplyr::case_when(
      stringr::str_detect(team,'angels') ~ 'ANA',
      stringr::str_detect(team,'dbacks') ~ 'ARI',
      stringr::str_detect(team,'orioles') ~ 'BAL',
      stringr::str_detect(team,'red sox') ~ 'BOS',
      stringr::str_detect(team,'cubs') ~ 'CHC',
      stringr::str_detect(team,'reds') ~ 'CIN',
      stringr::str_detect(team,'indians') ~ 'CLE',
      stringr::str_detect(team,'guardians') ~ 'CLE',
      stringr::str_detect(team,'rockies') ~ 'COL',
      stringr::str_detect(team,'tigers') ~ 'DET',
      stringr::str_detect(team,'astros') ~ 'HOU',
      stringr::str_detect(team,'royals') ~ 'KCR',
      stringr::str_detect(team,'dodgers') ~ 'LAD',
      stringr::str_detect(team,'nationals') ~ 'WSN',
      stringr::str_detect(team,'mets') ~ 'NYM',
      stringr::str_detect(team,'athletics') ~ 'OAK',
      stringr::str_detect(team,'pirates') ~ 'PIT',
      stringr::str_detect(team,'padres') ~ 'SPD',
      stringr::str_detect(team,'mariners') ~ 'SEA',
      stringr::str_detect(team,'giants') ~ 'SFG',
      stringr::str_detect(team,'cardinals') ~ 'STL',
      stringr::str_detect(team,'rays') ~ 'TBD',
      stringr::str_detect(team,'rangers') ~ 'TEX',
      stringr::str_detect(team,'blue jays') ~ 'TOR',
      stringr::str_detect(team,'twins') ~ 'MIN',
      stringr::str_detect(team,'phillies') ~ 'PHI',
      stringr::str_detect(team,'braves') ~ 'ATL',
      stringr::str_detect(team,'white sox') ~ 'CHW',
      stringr::str_detect(team,'marlins') ~ 'FLA',
      stringr::str_detect(team,'yankees') ~ 'NYY',
      stringr::str_detect(team,'brewers') ~ 'MIL'
    )

  } else if (type == 'schedule') {

    dplyr::case_when(
      stringr::str_detect(team,'angels') ~ 'ANA',
      stringr::str_detect(team,'dbacks') ~ 'ARI',
      stringr::str_detect(team,'orioles') ~ 'BAL',
      stringr::str_detect(team,'red sox') ~ 'BOS',
      stringr::str_detect(team,'cubs') ~ 'CHN',
      stringr::str_detect(team,'reds') ~ 'CIN',
      stringr::str_detect(team,'indians') ~ 'CLE',
      stringr::str_detect(team,'guardians') ~ 'CLE',
      stringr::str_detect(team,'rockies') ~ 'COL',
      stringr::str_detect(team,'tigers') ~ 'DET',
      stringr::str_detect(team,'astros') ~ 'HOU',
      stringr::str_detect(team,'royals') ~ 'KCA',
      stringr::str_detect(team,'dodgers') ~ 'LAN',
      stringr::str_detect(team,'nationals') ~ 'WAS',
      stringr::str_detect(team,'mets') ~ 'NYN',
      stringr::str_detect(team,'athletics') ~ 'OAK',
      stringr::str_detect(team,'pirates') ~ 'PIT',
      stringr::str_detect(team,'padres') ~ 'SDN',
      stringr::str_detect(team,'mariners') ~ 'SEA',
      stringr::str_detect(team,'giants') ~ 'SFN',
      stringr::str_detect(team,'cardinals') ~ 'SLN',
      stringr::str_detect(team,'rays') ~ 'TBA',
      stringr::str_detect(team,'rangers') ~ 'TEX',
      stringr::str_detect(team,'blue jays') ~ 'TOR',
      stringr::str_detect(team,'twins') ~ 'MIN',
      stringr::str_detect(team,'phillies') ~ 'PHI',
      stringr::str_detect(team,'braves') ~ 'ATL',
      stringr::str_detect(team,'white sox') ~ 'CHA',
      stringr::str_detect(team,'marlins') ~ 'FLA',
      stringr::str_detect(team,'yankees') ~ 'NYA',
      stringr::str_detect(team,'brewers') ~ 'MIL'
    )

  }

}
