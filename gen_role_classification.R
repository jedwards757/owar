# estimate player role within a given game
source('db_info.R')
require(tidyverse)
require(RMySQL)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

support = c('Lucio','Moira','Brigitte','Zenyatta','Mercy','Ana','Baptiste')
tank = c('Reinhardt','Sigma','Wrecking Ball','Zarya','Winston','D.Va','Roadhog','Orisa')
damage = c('Ashe','Bastion','Doomfist','Echo','Genji','Hanzo','Junkrat','McCree','Mei','Pharah','Reaper','Soldier: 76','Sombra','Symmetra','Torbj','Tracer','Widowmaker')

rs = dbSendQuery(mydb, "SELECT map_id, player, team, hero, stat_amount FROM owl.player_stats WHERE stat_name = 'Time Played' AND NOT hero = 'All Heroes'")
time_on_hero = dbFetch(rs,n=-1)
dbClearResult(rs)

dbWriteTable(mydb, 'player_stats_roles',
time_on_hero %>%
  mutate(role = case_when(hero %in% support~'support',
                           hero %in% damage~'damage',
                           hero %in% tank~'tank',
                           T~NA_character_)) %>%
  group_by(player, map_id, team, role) %>%
  summarize(time_on_role = sum(stat_amount)) %>%
  arrange(-time_on_role) %>%
  filter(row_number() == 1) %>%
  select(-c(time_on_role)),overwrite = T, append = F, row.names = F)