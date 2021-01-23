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

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

# rs = dbSendQuery(mydb,"SELECT match_id, map_id, team, stat_name, stat_amount FROM owl.player_stats WHERE hero = 'All Heroes' AND")
# team_stats = dbFetch(rs,n=-1)
# dbClearResult(rs)
# 
# team_stats = team_stats %>%
#   mutate(stat_name = slugify(stat_name)) %>%
#   group_by(map_id, team, stat_name) %>%
#   summarize(stat_amount = sum(stat_amount)) %>%
#   pivot_wider(names_from = stat_name, values_from = stat_amount) %>%
#   relocate(c(objective_time, time_building_ultimate, time_elapsed_per_ultimate_earned, time_holding_ultimate, average_time_alive, time_alive, weapon_accuracy, time_played),.after=last_col()) %>%
#   mutate(across(all_damage_done:time_alive,~replace_na(.x,0)),
#          across(all_damage_done:time_alive,~{. * 600 / time_played}, .names="{.col}_per_ten"),
#          time_played = time_played / 6,
#          melee_percentage_of_final_blows = melee_final_blows / final_blows,
#          weapon_accuracy = weapon_accuracy / 6)

rs = dbSendQuery(mydb,"SELECT 
    map_type,
    owl.player_stats.player,
    owl.player_stats.map_id,
    owl.player_stats.team,
    stat_name,
    stat_amount,
    role
FROM
    owl.player_stats
        INNER JOIN
    owl.player_stats_roles ON owl.player_stats_roles.map_id = owl.player_stats.map_id
        AND owl.player_stats_roles.player = owl.player_stats.player
WHERE
    hero = 'All Heroes'
        AND stat_name IN ('Hero Damage Done' , 'Deaths',
        'Eliminations',
        'Final Blows',
        'Solo Kills',
        'Defensive Assists',
        'Offensive Assists',
        'Damage Blocked',
        'Time Played')")
team_stats = dbFetch(rs,n=-1)
dbClearResult(rs)

team_stats = team_stats %>%
  mutate(stat_name = slugify(stat_name)) %>%
  pivot_wider(names_from = stat_name, values_from = stat_amount, values_fn = {sum}) %>%
  mutate(across(everything(),~replace_na(.x,0)),
         assisted_kills = final_blows - solo_kills) %>%
  group_by(map_id, map_type, team, role) %>%
  summarize(across(where(is.numeric),sum),
            players_in_role = n()) %>%
  mutate(across(!starts_with('players_in_role') & where(is.numeric),~{.x / players_in_role}),
         across(!starts_with('time_played') & where(is.numeric),~{.x * 600 / time_played})) %>%
  select(-c(time_played, players_in_role)) %>%
  pivot_wider(names_from = role, values_from = where(is.numeric)) %>%
  mutate(across(everything(),~replace_na(.x,0)))

rs = dbSendQuery(mydb,"SELECT stage, map_id, patch, team_one_name, team_two_name, map_winner, team_one_map_score, team_two_map_score, first_attacker from owl.results_by_map")
map_results = dbFetch(rs,n=-1)
dbClearResult(rs)

main_df = map_results %>% # using first attacker as the pivot column
  mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                    T~team_one_name)) %>%
  left_join(team_stats,by=c('map_id','first_attacker' = 'team')) %>%
  filter(map_winner != 'draw') %>%
  mutate(win = as.numeric(first_attacker == map_winner),
         first_attacker_flg = 1) %>%
  bind_rows(map_results %>%
              mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                                T~team_one_name)) %>%
              left_join(team_stats,by=c('map_id','first_defender' = 'team')) %>%
              filter(map_winner != 'draw') %>%
              mutate(win = as.numeric(first_defender == map_winner),
                     first_attacker_flg = -1)) %>%
  as_tibble() %>%
  mutate(year = case_when(str_detect(stage,'2018')~'2018',
                          str_detect(stage,'2019')~'2019',
                          str_detect(stage,'2020')~'2020'))

coef_over_time = function(mycoef){
  
  model_coef = data.frame()
  
  for(myyear in c(2018:2020)){
    stage_model = glm(win ~ map_type:first_attacker_flg + deaths_damage +
                  deaths_support +
                  deaths_tank +
                  final_blows_damage +
                  final_blows_support +
                  final_blows_tank +
                  healing_done_support + 0,
                data = main_df %>%
                  filter(str_detect(stage,as.character(myyear))),
                family = binomial())
    coef = broom::tidy(stage_model)
    coef$year = as.character(myyear)
    model_coef = bind_rows(model_coef, coef)
  }
  
  model_coef %>%
    filter(term == mycoef) %>%
    ggplot(aes(x = year, y=estimate, fill=year))+
    geom_bar(stat="identity",color="black",
             position = position_dodge()) + 
    geom_errorbar(aes(ymin=estimate-std.error,ymax=estimate+std.error),width=0.2,
                  position=position_dodge(0.9))
}

# model = glm(win ~ map_type:first_attacker_flg + deaths_damage +
#               deaths_support +
#               deaths_tank +
#               final_blows_damage +
#               final_blows_support +
#               final_blows_tank +
#               healing_done_support + 0,data = main_df,
#             family = binomial())

model = glm(win ~ map_type:first_attacker_flg + 
                deaths_damage:year+ 
                deaths_support:year + 
                deaths_tank:year + 
                final_blows_damage:year + 
                final_blows_support:year + 
                final_blows_tank:year +
                0,data = main_df,
              family = binomial())

dbDisconnect(mydb)