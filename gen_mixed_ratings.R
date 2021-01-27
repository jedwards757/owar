# generate mixed ratings

source('db_info.R')
require(tidyverse)
require(RMySQL)
require(ggplot2)
require(lme4)
require(glmnet)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

on_off_players = data.frame()
on_off_lineups = data.frame()

for(season in c(2018:2020)){
  rs = dbSendQuery(mydb,"SELECT player, map_id, team, map_type, subrole FROM owl.player_stats_subroles")
  player_roles = dbFetch(rs,n=-1) %>%
    mutate(subrole = case_when(subrole %in% c('hitscan','projectile')~'DPS',
                                    T~subrole))
  dbClearResult(rs)
  
  rs = dbSendQuery(mydb,"SELECT stage, map_id, patch, team_one_name, team_two_name, map_winner, team_one_map_score, team_two_map_score, first_attacker from owl.results_by_map")
  map_results = dbFetch(rs,n=-1)
  dbClearResult(rs)
  
  main_df = map_results %>% # using first attacker as the pivot column
    mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                      T~team_one_name)) %>%
    left_join(player_roles,by=c('map_id','first_attacker' = 'team')) %>%
    filter(map_winner != 'draw') %>%
    mutate(win = as.numeric(first_attacker == map_winner),
           first_attacker_flg = 1) %>%
    bind_rows(map_results %>%
                mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                                  T~team_one_name)) %>%
                left_join(player_roles,by=c('map_id','first_defender' = 'team')) %>%
                filter(map_winner != 'draw') %>%
                mutate(win = as.numeric(first_defender == map_winner),
                       first_attacker_flg = -1)) %>%
    as_tibble() %>%
    mutate(year = case_when(str_detect(stage,'2018')~'2018',
                            str_detect(stage,'2019')~'2019',
                            str_detect(stage,'2020')~'2020'),
           player_team = case_when(first_attacker_flg == 1 ~ first_attacker,
                                   T~first_defender),
           opponent_team = case_when(first_attacker_flg == 1~first_defender,
                                     T~first_attacker)) %>%
    filter(year == season) %>%
    group_by(player_team, opponent_team, map_id, win, map_type, first_attacker_flg) %>%
    arrange(player) %>%
    summarize(lineup = paste0(player, '_', subrole, collapse=';')) %>%
    ungroup()
  
  player_roles = map_results %>% # using first attacker as the pivot column
    mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                      T~team_one_name)) %>%
    left_join(player_roles,by=c('map_id','first_attacker' = 'team')) %>%
    filter(map_winner != 'draw') %>%
    mutate(win = as.numeric(first_attacker == map_winner),
           first_attacker_flg = 1) %>%
    bind_rows(map_results %>%
                mutate(first_defender = case_when(team_one_name == first_attacker~team_two_name,
                                                  T~team_one_name)) %>%
                left_join(player_roles,by=c('map_id','first_defender' = 'team')) %>%
                filter(map_winner != 'draw') %>%
                mutate(win = as.numeric(first_defender == map_winner),
                       first_attacker_flg = -1)) %>%
    as_tibble() %>%
    mutate(year = case_when(str_detect(stage,'2018')~'2018',
                            str_detect(stage,'2019')~'2019',
                            str_detect(stage,'2020')~'2020'),
           player_team = case_when(first_attacker_flg == 1 ~ first_attacker,
                                   T~first_defender),
           opponent_team = case_when(first_attacker_flg == 1~first_defender,
                                     T~first_attacker),
           played = 1) %>%
    filter(year == season) %>%
    mutate(player_role = paste0(player,'_',subrole)) %>%
    pull(player_role) %>%
    unique()
  
  main_df = main_df %>%
    inner_join(main_df %>%
                 select(-c(win,opponent_team, map_type, first_attacker_flg)),by=c('map_id','opponent_team' = 'player_team'), suffix=c('','_opponent'))
  
  # generate scores for lineups
  
  model = glmer(win ~ map_type:first_attacker_flg + (1|lineup) + (1|lineup_opponent) + 0,
                data = main_df,
                family = binomial())
  
  # harvest random coefficients
  
  lineup_ranefs = ranef(model) %>% 
    as.data.frame() %>%
    group_by(grp) %>%
    summarize(effect = mean(case_when(grpvar == 'lineup'~condval,
                                      T~-1*condval)))
  
  lineup_ranefs = main_df %>%
    select(lineup,player_team) %>%
    group_by(lineup,player_team) %>%
    summarize(n = n()) %>%
    inner_join(lineup_ranefs,by=c('lineup' = 'grp'))
  
  A = map_dfc(player_roles, ~as.numeric(str_detect(lineup_ranefs$lineup, .x))) %>% as.matrix()
  B = lineup_ranefs$effect
  W = lineup_ranefs$n
  lambdas <- 10^seq(3, -2, by = -.1)
  
  cv_fit = cv.glmnet(A, B, alpha = 0, lambda = lambdas, weights = W, intercept = F)
  fit = glmnet(A, B, alpha = 0, lambda = cv_fit$lambda.min, weights = W, intercept = F)
  
  player_effects = data.frame(players = player_roles, score = coef(fit) %>% as.vector() %>% tail(-1))
  
  on_off_players = bind_rows(on_off_players, player_effects %>% mutate(year = season))
  on_off_lineups = bind_rows(on_off_lineups, lineup_ranefs %>% select(c(lineup,n,estimate = effect, team = player_team)) %>% mutate(year = season))
}

dbWriteTable(mydb, 'on_off_player_scores', on_off_players, overwrite = T, append = F, row.names = F)
dbWriteTable(mydb, 'on_off_lineup_scores', on_off_lineups, overwrite = T, append = F, row.names = F)

dbDisconnect(mydb)