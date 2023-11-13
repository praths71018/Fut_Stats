CREATE VIEW RK AS
SELECT m.Club_Name, m.Manager, m.NATIONALITY AS Nationality, p.MP, p.W, p.D, p.L, p.GF, p.GA, p.GD, p.Pts
FROM points_table p
INNER JOIN manager m ON m.Club_name = p.Club_name
ORDER BY p.Pts DESC;