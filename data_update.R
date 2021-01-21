source('db_info.R')
require(tidyverse)
require(RSelenium)
require(rvest)
require(RMySQL)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

rD = rsDriver(browser="firefox")
remDr = rD$client
remDr$navigate("https://overwatchleague.com/en-us/statslab")
file_links = remDr$findElement(using="class",value="sl-downloads")$getElementAttribute("outerHTML") %>%
  .[[1]] %>%
  read_html() %>%
  html_nodes("a") %>%
  html_attr("href")
remDr$close()
system("taskkill /im java.exe /f", intern=FALSE, ignore.stdout=FALSE)
rm(rD)
gc()

file_names = str_extract_all(file_links,"\\/(?:.(?!\\/))+$(.*?)",simplify=T) %>%
  str_replace_all('/','data/')
  
safe_download = safely(~ download.file(.x , .y, mode = "wb"))
walk2(file_links, file_names, safe_download)

pwalk(list(file_names), unzip, exdir=paste0(getwd(),'/data/csvs'))

dbWriteTable(mydb, "match_map_stats", read_csv('data/csvs/match_map_stats.csv',col_types = cols(round_start_time = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                                                round_end_time = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                                                                stage = col_character(), match_id = col_character(), 
                                                                                                game_number = col_character(), match_winner = col_character(), 
                                                                                                map_winner = col_character(), map_loser = col_character(), 
                                                                                                map_name = col_character(), map_round = col_character(), 
                                                                                                winning_team_final_map_score = col_double(), 
                                                                                                losing_team_final_map_score = col_double(), 
                                                                                                control_round_name = col_character(), 
                                                                                                attacker = col_character(), defender = col_character(), 
                                                                                                team_one_name = col_character(), 
                                                                                                team_two_name = col_character(), 
                                                                                                attacker_payload_distance = col_double(), 
                                                                                                defender_payload_distance = col_double(), 
                                                                                                attacker_time_banked = col_double(), 
                                                                                                defender_time_banked = col_double(), 
                                                                                                attacker_control_perecent = col_double(), 
                                                                                                defender_control_perecent = col_double(), 
                                                                                                attacker_round_end_score = col_double(), 
                                                                                                defender_round_end_score = col_double())) %>%
               mutate(map_id = paste0(match_id,'_',game_number)) %>%
               filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = T, append = F, row.names = F)

csvs = list.files(path='data/csvs/',pattern="*.csv")
csvs = csvs[!(csvs %in% c('match_map_stats.csv','phs_2020_2.csv'))] %>% # phs_2020_2 is manually excluded because of error with datetime
  paste0('data/csvs/',.)

# dbSendQuery(mydb,"DROP TABLE `owl`.`player_stats`")

dbWriteTable(mydb, "player_stats",read_csv('data/csvs/phs_2020_2.csv',col_types = cols(start_time = col_datetime(format = "%Y-%m-%d %H:%M:%S"), 
                                                   esports_match_id = col_character(), tournament_title = col_character(), 
                                                   map_type = col_character(), map_name = col_character(), 
                                                   player_name = col_character(), team_name = col_character(), 
                                                   stat_name = col_character(), hero_name = col_character(), 
                                                   stat_amount = col_double())) %>%
               rename(match_id = esports_match_id,
                      stage = tournament_title,
                      player = player_name,
                      team = team_name,
                      hero = hero_name) %>%
               mutate(hero = case_when(hero == 'Lúcio'~'Lucio',
                                       hero == 'Torbj'~'Torbjorn',
                                       T~hero)) %>%
               group_by(match_id) %>%
               arrange(start_time) %>%
               mutate(map_id = paste0(match_id,'_',as.numeric(factor(start_time)))) %>%
               filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = T, append = F, row.names = F)

for(csv in csvs){
  if(csv == "data/csvs/phs_2019_stage_1.csv"){
    dbWriteTable(mydb, "player_stats",read_csv(csv,col_types = cols(pelstart_time = col_datetime(format = "%m/%d/%Y %H:%M"), 
                                                                    match_id = col_character(), stage = col_character(), 
                                                                    map_type = col_character(), map_name = col_character(), 
                                                                    player = col_character(), team = col_character(), 
                                                                    stat_name = col_character(), hero = col_character(), 
                                                                    stat_amount = col_double())) %>%
                   rename(start_time = pelstart_time) %>%
                   mutate(hero = case_when(hero == 'Lúcio'~'Lucio',
                                           hero == 'Torbj'~'Torbjorn',
                                           T~hero)) %>%
                   group_by(match_id) %>%
                   arrange(start_time) %>%
                   mutate(map_id = paste0(match_id,'_',as.numeric(factor(start_time)))) %>%
                   filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = F, append = T, row.names = F)
  } else if(csv == "data/csvs/phs_2019_stage_2.csv"){
    dbWriteTable(mydb, "player_stats",read_csv(csv,col_types = cols(start_time = col_datetime(format = "%m/%d/%Y %H:%M"), 
                                                                    match_id = col_character(), stage = col_character(), 
                                                                    map_type = col_character(), map_name = col_character(), 
                                                                    player = col_character(), team = col_character(), 
                                                                    stat_name = col_character(), hero = col_character(), 
                                                                    stat_amount = col_double())) %>%
                   mutate(hero = case_when(hero == 'Lúcio'~'Lucio',
                                           hero == 'Torbj'~'Torbjorn',
                                           T~hero)) %>%
                   group_by(match_id) %>%
                   arrange(start_time) %>%
                   mutate(map_id = paste0(match_id,'_',as.numeric(factor(start_time)))) %>%
                   filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = F, append = T, row.names = F)
  } else if(csv == "data/csvs/phs_2020_1.csv"){
    dbWriteTable(mydb, "player_stats",read_csv('data/csvs/phs_2020_1.csv',col_types = cols(start_time = col_datetime(format = "%m/%d/%Y %H:%M"), 
                                                                                           esports_match_id = col_character(), tournament_title = col_character(), 
                                                                                           map_type = col_character(), map_name = col_character(), 
                                                                                           player_name = col_character(), team_name = col_character(), 
                                                                                           stat_name = col_character(), hero_name = col_character(), 
                                                                                           stat_amount = col_double())) %>%
                   rename(match_id = esports_match_id,
                          stage = tournament_title,
                          player = player_name,
                          team = team_name,
                          hero = hero_name) %>%
                   mutate(hero = case_when(hero == 'Lúcio'~'Lucio',
                                           hero == 'Torbj'~'Torbjorn',
                                           T~hero)) %>%
                   group_by(match_id) %>%
                   arrange(start_time) %>%
                   mutate(map_id = paste0(match_id,'_',as.numeric(factor(start_time)))) %>%
                   filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = F, append = T, row.names = F)
  } else {
    dbWriteTable(mydb, "player_stats",read_csv(csv,col_types = cols(start_time = col_datetime(format = "%m/%d/%Y %H:%M"), 
                                                                    match_id = col_character(), stage = col_character(), 
                                                                    map_type = col_character(), map_name = col_character(), 
                                                                    player = col_character(), team = col_character(), 
                                                                    stat_name = col_character(), hero = col_character(), 
                                                                    stat_amount = col_double())) %>%
                   mutate(hero = case_when(hero == 'Lúcio'~'Lucio',
                                           hero == 'Torbj'~'Torbjorn',
                                           T~hero)) %>%
                   group_by(match_id) %>%
                   arrange(start_time) %>%
                   mutate(map_id = paste0(match_id,'_',as.numeric(factor(start_time)))) %>%
                   filter(!(stage %in% c('OWL APAC All-Stars','OWL North America All-Stars'))), overwrite = F, append = T, row.names = F)
  }
}
gc()
dbDisconnect(mydb)