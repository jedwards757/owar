# assigns map win credit to each player

# aggregates stats data by team and match
source('db_info.R')
require(tidyverse)
require(RMySQL)
require(ggplot2)
require(lme4)

slugify <- function(x, alphanum_replace="", space_replace="_", tolower=TRUE) {
  x <- gsub("[^[:alnum:] ]", alphanum_replace, x)
  x <- gsub(" ", space_replace, x)
  if(tolower) { x <- tolower(x) }
  
  return(x)
}

# acquire and clean data

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

rs = dbSendQuery(mydb,"SELECT 
    owl.player_stats.map_type,
    owl.player_stats.player,
    owl.player_stats.map_id,
    owl.player_stats.team,
    stat_name,
    stat_amount,
    subrole
FROM
    owl.player_stats
        INNER JOIN
    owl.player_stats_subroles ON owl.player_stats_subroles.map_id = owl.player_stats.map_id
        AND owl.player_stats_subroles.player = owl.player_stats.player
WHERE
    owl.player_stats.hero = 'All Heroes'
        AND stat_name IN ('Deaths',
        'Final Blows')")
player_stats = dbFetch(rs,n=-1)
dbClearResult(rs)

player_stats = player_stats %>%
  mutate(stat_name = slugify(stat_name)) %>%
  pivot_wider(names_from = stat_name, values_from = stat_amount, values_fn = {sum}) %>%
  mutate(across(everything(),~replace_na(.x,0))) %>%
  group_by(map_id, map_type, player, team, subrole) %>%
  summarize(across(where(is.numeric),sum)) %>%
  mutate(subrole2 = subrole) %>%
  pivot_wider(names_from = subrole2, values_from = where(is.numeric)) %>%
  mutate(across(everything(),~replace_na(.x,0)))

rs = dbSendQuery(mydb,"SELECT stage, map_id, patch, team_one_name, team_two_name, map_winner, team_one_map_score, team_two_map_score, first_attacker from owl.results_by_map")
map_results = dbFetch(rs,n=-1)
dbClearResult(rs)

main_df = map_results %>% # using first attacker as the pivot column
  mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                    T~team_one_name)) %>%
  left_join(player_stats,by=c('map_id','first_attacker' = 'team')) %>%
  mutate(win = as.numeric(first_attacker == map_winner),
         first_attacker_flg = 1) %>%
  bind_rows(map_results %>%
              mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                                T~team_one_name)) %>%
              left_join(player_stats,by=c('map_id','first_defender' = 'team')) %>%
              mutate(win = as.numeric(first_defender == map_winner),
                     first_attacker_flg = -1)) %>%
  as_tibble() %>%
  mutate(year = case_when(str_detect(stage,'2018')~'2018',
                          str_detect(stage,'2019')~'2019',
                          str_detect(stage,'2020')~'2020')) %>%
  mutate(player_team = case_when(first_attacker_flg == 1~first_attacker,
                                 T~first_defender))

# generate individual player predictions

stat_model = readRDS('models/stat_model.RDS')

main_df$score = predict(stat_model,main_df %>% mutate(first_attacker_flg = 0))

main_df = main_df %>%
  group_by(subrole) %>%
  mutate(scaled_score = scale(score))

main_df$map_adv_score = predict(stat_model,main_df %>%
                                  mutate(across(starts_with('deaths'),~0),
                                         across(starts_with('final_blows'),~0)))

main_df$map_adv_score = case_when(main_df$map_type == 'CONTROL'~0,
                                  T~main_df$map_adv_score)

# re-scale win prob values to fall between 1 and 0

dbWriteTable(mydb,'stat_scores',main_df %>%
               select(c(year, stage, map_id, patch, map_type, player_team, subrole, player, score, scaled_score, map_adv_score)),overwrite = T,append = F, row.names = F)

dbDisconnect(mydb)