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

operation_id = 0 #<------------------------- DELETE ------

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

# START OPERATIONS ----

# Determining the year of the most recent season

if (Sys.Date() >= as.Date(glue(lubridate::year(Sys.Date()),"-03-31"))) {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date()))

} else {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date())-1)

}

schedule_data <- load_schedule(years = c(2017:2021)) %>% rbindlist() #<------------------------- DELETE ------

schedule_transform <- schedule_data %>%
  mutate(
    away_team_id = helper_team_id(team = away_team_name, type = 'id'),
    home_team_id = helper_team_id(team = home_team_name, type = 'id'),
    away_team_abbrv = helper_team_id(team = away_team_name, type = 'abbrv'),
    home_team_abbrv = helper_team_id(team = home_team_name, type = 'abbrv'),
    game_id = paste0(helper_team_id(team = schedule_data$home_team_name, type = 'schedule'),
                     str_remove_all(as.character(schedule_data$date),'-')
                     ),
    operation_id = operation_id,
    operation_name = operation_name,
    operation_start_dtm = operation_start_dtm
)

schedule_grouped <- schedule_transform %>% group_by(game_id, date)

schedule_ranked <- schedule_grouped %>%
  mutate(rank = rank(date,ties.method = 'first')) %>%
  left_join(., schedule_grouped %>% count() %>% ungroup() %>% select(-date), by = 'game_id') %>%
  mutate(game_id = if_else(n == 1, paste0(game_id,0),paste0(game_id,rank))) %>%
  select(-rank,-n)

saveRDS(schedule_data, file = 'data/schedule_data.RDS')






