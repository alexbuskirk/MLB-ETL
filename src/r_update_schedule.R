# SETUP ----

## For Task Scheduler  ----
setwd('C:/Users/alexb/Desktop/projects/MLB-ETL')

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

# START OPERATIONS ----

# Determining the year of the most recent season

if (Sys.Date() >= as.Date(glue(lubridate::year(Sys.Date()),"-03-31"))) {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date()))

} else {

  schedule_data <- load_schedule(years = lubridate::year(Sys.Date())-1)

}

schedule_transform <- schedule_data %>%
  rbindlist() %>%
  mutate(
    away_team_id = as.integer(helper_team_id(team = away_team_name, type = 'id')),
    home_team_id = as.integer(helper_team_id(team = home_team_name, type = 'id')),
    away_team_abbrv = helper_team_id(team = away_team_name, type = 'abbrv'),
    home_team_abbrv = helper_team_id(team = home_team_name, type = 'abbrv'),
    game_id = paste0(helper_team_id(team = home_team_name, type = 'schedule'),
                     str_remove_all(as.character(game_date),'-')
                     ),
    season = as.integer(season),
    operation_id = as.integer(operation_id),
    last_update_dtm = operation_start_dtm
)

schedule_grouped <- schedule_transform %>% group_by(game_id, game_date)

schedule_ranked <- schedule_grouped %>%
  mutate(rank = rank(game_date,ties.method = 'first')) %>%
  left_join(., schedule_grouped %>% count() %>% ungroup() %>% select(-game_date), by = 'game_id') %>%
  mutate(game_id = if_else(n == 1, paste0(game_id,0),paste0(game_id,rank))) %>%
  ungroup() %>%
  select(-rank,-n)

schedule_reorder <- schedule_ranked %>%
  select(game_id
         ,game_date
         ,away_team_name
         ,away_team_id
         ,away_team_score
         ,away_team_win_loss
         ,home_team_name
         ,home_team_id
         ,home_team_score
         ,home_team_win_loss
         ,season
         ,operation_id
         ,last_update_dtm
         )

season_to_update <- max(schedule_reorder$season)

delete <- dbSendQuery(odbc_connection_pc,
              glue('DELETE FROM mlb.dbo.schedule WHERE season={season_to_update}'))

dbClearResult(delete)

dbWriteTable(
  odbc_connection_pc,
  value = schedule_reorder,
  name = 'schedule',
  append = TRUE#,
  #filed.type = class_type
)

  # release the prepared statement

function_logging()


