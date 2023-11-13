desc Football_Stats.Assists;
desc Football_Stats.Goals;
desc Football_Stats.Clean_Sheets;
SELECT A.P_Name
FROM Football_Stats.Assists A
LEFT JOIN Football_Stats.player_stats P ON A.P_Name = P.P_Name
WHERE P.P_Name IS NULL;

ALTER TABLE Football_Stats.Assists
    ADD FOREIGN KEY (P_Name) REFERENCES player_stats(P_Name);
    
SELECT A.P_Name
FROM Football_Stats.Goals A
LEFT JOIN Football_Stats.player_stats P ON A.P_Name = P.P_Name
WHERE P.P_Name IS NULL;    
    
ALTER TABLE Football_Stats.Goals
    ADD FOREIGN KEY (P_Name) REFERENCES player_stats(P_Name);

SELECT A.P_Name
FROM Football_Stats.Clean_Sheets A
LEFT JOIN Football_Stats.player_stats P ON A.P_Name = P.P_Name
WHERE P.P_Name IS NULL; 
    
ALTER TABLE Football_Stats.Clean_Sheets
    ADD FOREIGN KEY (P_Name) REFERENCES player_stats(P_Name);
    
SELECT manager.Club_Name
FROM Football_Stats.manager
LEFT JOIN Football_Stats.points_table points ON manager.Club_Name = points.Club_Name
WHERE points.Club_Name IS NULL;

SELECT Inter_Club_Tour_Stats.Club_Name
FROM Football_Stats.Inter_Club_Tour_Stats
LEFT JOIN Football_Stats.points_table points ON Inter_Club_Tour_Stats.Club_Name = points.Club_Name
WHERE points.Club_Name IS NULL;

desc Football_Stats.manager;

ALTER TABLE Football_Stats.Inter_Stats
    ADD FOREIGN KEY (P_Name) REFERENCES player_stats(P_Name);
    
SELECT A.P_Name
FROM Football_Stats.Inter_Stats A
LEFT JOIN Football_Stats.player_stats P ON A.P_Name = P.P_Name
WHERE P.P_Name IS NULL; 

SELECT A.Player
FROM Football_Stats.Inter_Club_Tour_Stats A
LEFT JOIN Football_Stats.player_stats P ON A.Player = P.P_Name
WHERE P.P_Name IS NULL; 

ALTER TABLE Inter_Club_Tour_Stats
    ADD FOREIGN KEY (Club_Name) REFERENCES points_table(Club_Name);
