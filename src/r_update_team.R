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
operation_id <- dbGetQuery(
  conn = odbc_connection_pc,
  'select max(operation_id) from mlb.dbo.time_tracking'
) %>% as.numeric()

operation_start_dtm <- Sys.time()
operation_name <- 'update_schedule'
operation_id <- operation_id + 1

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

## Test internet connection ----
if (havingIP() == FALSE) {

  log_entry('Cannot connect to the internet')
  Sys.sleep(120)

  if (havingIP() == FALSE) {

    log_entry('Still cannot connect to the internet')
    stop()

  }

} # End if: testing internet connection

## Start Operations ----

team_table <- read.csv('data/team_table.csv') %>%
  mutate(operation_id = operation_id,
         last_update_dtm = operation_start_dtm)

dbWriteTable(
  odbc_connection_pc,
  value = team_table,
  name = 'team',
  append = TRUE#,
  #filed.type = class_type
)

function_logging()
