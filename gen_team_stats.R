# aggregates stats data by team and match
source('db_info.R')
require(tidyverse)
require(RMySQL)

slugify <- function(x, alphanum_replace="", space_replace="_", tolower=TRUE) {
  x <- gsub("[^[:alnum:] ]", alphanum_replace, x)
  x <- gsub(" ", space_replace, x)
  if(tolower) { x <- tolower(x) }
  
  return(x)
}

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

rs = dbSendQuery(mydb,"SELECT match_id, map_id, team, stat_name, stat_amount FROM owl.player_stats where hero = 'All Heroes'")
team_stats = dbFetch(rs,n=-1)
dbClearResult(rs)

team_stats = team_stats %>%
  mutate(stat_name = slugify(stat_name)) %>%
  group_by(map_id, team, stat_name) %>%
  summarize(stat_amount = sum(stat_amount)) %>%
  pivot_wider(names_from = stat_name, values_from = stat_amount) %>%
  mutate(across(where(is.numeric),~replace_na(.x,0)),
         across(!contains('time'),~{. * 100 / time_played}, names = "{.col}_per_ten"),
         melee_percentage_of_final_blows = melee_final_blows / final_blows)

rs = dbSendQuery(mydb,"SELECT match_id, map_id, team_one_name, team_two_name, map_winner, team_one_map_score, team_two_map_score, first_attacker from owl.results_by_map")
map_results = dbFetch(rs,n=-1)
dbClearResult(rs)

main_df = map_results %>%
  mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                    T~team_one_name)) %>%
  left_join(team_stats,by=c('map_id','first_attacker' = 'team')) %>%
  left_join(team_stats,by=c('map_id','first_defender' = 'team'),suffix=c(".attacker",'.defender'))

dbDisconnect(mydb)