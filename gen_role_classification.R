# estimate player role within a given game
source('db_info.R')
require(tidyverse)
require(RMySQL)

mydb = dbConnect(MySQL(), user=user, password=password, dbname='owl', host=host)

support = c('Lucio','Moira','Brigitte','Zenyatta','Mercy','Ana','Baptiste')
tank = c('Reinhardt','Sigma','Wrecking Ball','Zarya','Winston','D.Va','Roadhog','Orisa')
damage = c('Ashe','Bastion','Doomfist','Echo','Genji','Hanzo','Junkrat','McCree','Mei','Pharah','Reaper','Soldier: 76','Sombra','Symmetra','Torbj','Tracer','Widowmaker')

hitscan = c('Ashe','Bastion','McCree','Reaper','Soldier: 76','Sombra','Symmetra','Tracer','Widowmaker')
projectile = c('Doomfist','Echo','Genji','Hanzo','Junkrat','Mei','Pharah','Torbj')

rs = dbSendQuery(mydb, "SELECT 
    map_id, player, team, hero, stat_amount
FROM
    owl.player_stats
WHERE
    stat_name = 'Time Played'
        AND NOT hero = 'All Heroes'")
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

dbWriteTable(mydb,'player_stats_subroles',
             time_on_hero %>%
               group_by(player, map_id, team, hero) %>%
               summarize(time_on_hero = sum(stat_amount)) %>%
               arrange(-time_on_hero) %>%
               filter(row_number() == 1) %>%
               select(-c(time_on_hero)) %>%
               left_join(time_on_hero %>%
                           group_by(player, map_id, team, hero) %>%
                           summarize(time_on_hero = sum(stat_amount)) %>%
                           arrange(-time_on_hero) %>%
                           filter(row_number() == 1) %>%
                           select(-c(time_on_hero)) %>%
                           group_by(map_id,team) %>%
                           summarize(heroes = paste(hero,collapse = ', '))) %>%
               arrange(map_id, team, player) %>%
               mutate(subrole = case_when(hero %in% hitscan~'hitscan',
                                          hero %in% projectile~'projectile',
                                          #lucio logic
                                          hero == 'Lucio' & (str_detect(heroes,'Moira') | (str_detect(heroes,'Mercy') & !(str_detect(heroes,'Pharah') | str_detect(heroes,'Echo'))))~'off-support',
                                          hero == 'Lucio' & !(str_detect(heroes,'Moira') | str_detect(heroes,'Mercy') | str_detect(heroes,'Brigitte') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Ana') | str_detect(heroes,'Baptiste')) ~ 'main-support',
                                          hero == 'Lucio' ~ 'main-support',
                                          # moira logic
                                          hero == 'Moira' ~ 'main-support',
                                          # brigitte logic
                                          hero == 'Brigitte' & (str_detect(heroes,'Lucio') | str_detect(heroes,'Moira') | (str_detect(heroes,'Mercy') & !(str_detect(heroes,'Pharah') | str_detect(heroes,'Echo'))) | str_detect(heroes,'Ana') | str_detect(heroes,'Baptiste')) ~ 'off-support',
                                          hero == 'Brigitte' & !(str_detect(heroes,'Moira') | str_detect(heroes,'Mercy') | str_detect(heroes,'Lucio') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Ana') | str_detect(heroes,'Baptiste')) ~ 'main-support',
                                          hero == 'Brigitte' ~ 'main-support',
                                          # zen logic
                                          hero == 'Zenyatta' ~ 'off-support',
                                          # ana logic
                                          hero == 'Ana' & (str_detect(heroes,'Lucio') | str_detect(heroes,'Moira') | (str_detect(heroes,'Mercy') & !(str_detect(heroes,'Pharah') | str_detect(heroes,'Echo'))) | str_detect(heroes,'Baptiste'))~'off-support',
                                          hero == 'Ana' & !(str_detect(heroes,'Moira') | str_detect(heroes,'Mercy') | str_detect(heroes,'Lucio') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Brigitte') | str_detect(heroes,'Baptiste')) ~ 'main-support',
                                          hero == 'Ana' ~ 'main-support',
                                          # bap logic
                                          hero == "Baptiste" & (str_detect(heroes,'Lucio') | (str_detect(heroes,'Mercy') & !(str_detect(heroes,'Pharah') | str_detect(heroes,'Echo'))) | str_detect(heroes,'Moira')) ~ 'off-support',
                                          # hero == 'Baptiste' & !(str_detect(heroes,'Brigitte') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Ana') | (str_detect(heroes,'Mercy') & (str_detect(heroes,'Pharah') | str_detect(heroes,'Echo'))))~'main-support',
                                          hero == 'Baptiste' & !(str_detect(heroes,'Moira') | str_detect(heroes,'Mercy') | str_detect(heroes,'Lucio') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Brigitte') | str_detect(heroes,'Ana')) ~ 'main-support',
                                          hero == 'Baptiste' ~ 'main-support',
                                          # mercy logic
                                          hero == 'Mercy' & (str_detect(heroes,'Pharah') | str_detect(heroes,'Echo')) & !str_detect(heroes,'Zenyatta') & (str_detect(heroes,'Moira') | str_detect(heroes,'Lucio')) ~ 'off-support',
                                          hero == 'Mercy' & !(str_detect(heroes,'Moira') | str_detect(heroes,'Baptiste') | str_detect(heroes,'Lucio') | str_detect(heroes,'Zenyatta') | str_detect(heroes,'Mercy') | str_detect(heroes,'Brigitte')) ~ 'main-support',
                                          hero == 'Mercy' ~ 'main-support',
                                          hero %in% c('Reinhardt','Orisa','Winston') ~ 'main-tank',
                                          hero == 'Sigma' & !(str_detect(heroes,'Reinhardt') | str_detect(heroes,'Orisa') | str_detect(heroes,'Winston'))~'main-tank',
                                          hero == 'Sigma' ~ 'off-tank',
                                          hero == 'Wrecking Ball' & (str_detect(heroes,'Reinhardt') | str_detect(heroes,'Orisa') | str_detect(heroes,'Winston') | str_detect(heroes,'Sigma')) ~ 'off-tank',
                                          hero == 'Wrecking Ball' ~ 'main-tank',
                                          hero == 'Zarya' & (str_detect(heroes,'Reinhardt') | str_detect(heroes,'Orisa') | str_detect(heroes,'Winston') | str_detect(heroes,'Sigma') | str_detect(heroes,'Wrecking Ball')) ~ 'off-tank',
                                          hero == 'Zarya' ~ 'main-tank',
                                          hero == 'D.Va' & (str_detect(heroes,'Reinhardt') | str_detect(heroes,'Orisa') | str_detect(heroes,'Winston') | str_detect(heroes,'Sigma') | str_detect(heroes,'Wrecking Ball') | str_detect(heroes,'Zarya')) ~ 'off-tank',
                                          hero == 'D.Va' ~ 'main-tank',
                                          hero == 'Roadhog' ~ 'off-tank',
                                          T~NA_character_)),overwite = T, append = F, row.names = F)



dbDisconnect(mydb)