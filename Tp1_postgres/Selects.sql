EXPLAIN ANALYZE
SELECT * 
FROM etudiants 
inner join logs_connexions on id_etu = id_etu_fk
inner join cours on id_cours = id_cours_fk 
WHERE matiere = 'Deep Learning 1';

EXPLAIN ANALYZE
SELECT * 
FROM etudiants 
inner join logs_connexions on id_etu = id_etu_fk
inner join cours on id_cours = id_cours_fk 
WHERE id_cours = 43;


EXPLAIN ANALYZE
SELECT c.matiere, COUNT(i.id_etu_fk) AS nb_inscrits
FROM cours c
LEFT JOIN inscriptions i ON i.id_etu_fk IN (
    SELECT id_etu FROM etudiants
)
GROUP BY c.matiere
ORDER BY nb_inscrits desc;

EXPLAIN ANALYZE
SELECT c.matiere, COUNT(id_etu) AS nb_inscrits
FROM cours c
LEFT JOIN logs_connexions lc ON lc.id_cours_fk = c.id_cours 
LEFT JOIN etudiants e ON e.id_etu = lc.id_etu_fk
GROUP BY c.matiere
ORDER BY nb_inscrits desc;


EXPLAIN analyze
select * 
from inscriptions 
where date_insc = '2025-08-26';

EXPLAIN ANALYZE
SELECT * 
FROM etudiants
WHERE nom = 'Martin';