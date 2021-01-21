# generates game-level data from mapped dataframes for use in regression, elo, etc.
source('db_info.R')
require(tidyverse)
require(RMySQL)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

rs = dbSendQuery(mydb, "SELECT stage, match_id, map_id, game_number, map_name, team_one_name, team_two_name, match_winner, map_winner, winning_team_final_map_score, losing_team_final_map_score, attacker FROM owl.match_map_stats WHERE stage != 'OWL APAC All-Stars' OR stage != 'OWL North America All-Stars'")
match_map_stats = dbFetch(rs,n=-1)
dbClearResult(rs)

results_by_map = match_map_stats %>%
  group_by(stage, match_id, map_id) %>%
  summarize(team_one_name = first(team_one_name),
            team_two_name = first(team_two_name),
            match_winner = first(match_winner),
            map_winner = first(map_winner),
            game_number = first(game_number),
            team_one_map_score = ifelse(team_one_name == map_winner,first(winning_team_final_map_score),first(losing_team_final_map_score)),
            team_two_map_score = ifelse(team_one_name == map_winner,first(losing_team_final_map_score),first(winning_team_final_map_score)),
            first_attacker = first(attacker))
            
results_by_match = results_by_map %>%
  group_by(stage, match_id) %>%
  summarize(team_one_name = first(team_one_name),
            team_two_name = first(team_two_name),
            match_winner = first(match_winner),
            games = max(game_number),
            team_one_match_score = sum(team_one_name == map_winner),
            team_two_match_score = sum(team_two_name == map_winner)) %>%
  mutate(ties = as.numeric(games) - (team_one_match_score + team_two_match_score))

dbWriteTable(mydb,'results_by_map',results_by_map,overwrite = T, append = F, row.names = F)
dbWriteTable(mydb,'results_by_match',results_by_match,overwrite = T,append = F, row.names = F)
dbDisconnect(mydb)