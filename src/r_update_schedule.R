# SETUP ----

## For Task Scheduler  ----
setwd('C:/Users/catspur/Desktop/projects/MLB-ETL')

## Load functions and libraries ----
source(
  paste(
    getwd(),
    '/src/',
    'r_functions.R',
    sep = ''
  )
)

function_load_libraries()

## Create connection ----
function_odbc_set_up()

## Logging operations ----
operations_number <- dbGetQuery(
  conn = odbc_connection_pc,
  'select max(operations_number) from mlb.dbo.master_time_tracking'
) %>% as.numeric()

operations_number = 0 #<------------------------- DELETE ------

start_time <- Sys.time()
operations <- 'update_schedule'
operations_number <- operations_number + 1

## Create log file ----
log_file <- file(
  paste(
    getwd(),
    '/data/log/mlb_schedule_update_log/',
    str_replace_all(
      Sys.time(),
      '-| |:',
      '_'
    ),
    '.txt',
    sep = ''
  ),
  open = 'a'
)

# START OPERATIONS ----

# Determining the year of the most recent season

if (Sys.Date() >= as.Date(glue(lubridate::year(Sys.Date()),"-03-31"))) {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date()))

} else {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date())-1)

}

schedule_data <- load_schedule(years = c(2017:2021)) %>% rbindlist() #<------------------------- DELETE ------

schedule_data <- schedule_data %>%
  mutate(
    away_team_id = helper_team_id(t = away_team_name),
    home_team_id = helper_team_id(t = home_team_name),
)
