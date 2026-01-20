CREATE TABLE etudiants (
id_etu serial primary key,
prenom varchar(25),
nom varchar(25)
);

CREATE TABLE cours (
id_cours serial primary key,
matiere varchar(25) 
);

CREATE TABLE inscriptions (
date_insc TIMESTAMP not null,
id_etu_fk INT not null,
status boolean not null,
primary key (date_insc, id_etu_fk),
foreign key (id_etu_fk) references etudiants(id_etu)
);

CREATE TABLE logs_connexions (
id_etu_fk INT not null,
id_cours_fk INT not null,
date_log TIMESTAMP not null,
primary key (id_etu_fk, id_cours_fk, date_log),
foreign key (id_etu_fk) references etudiants(id_etu),
foreign key (id_cours_fk) references cours(id_cours)
);


INSERT INTO etudiants(prenom, nom)
SELECT
(ARRAY[
'Alice','Bob','Claire','David','Emma','Lucas','Léa','Hugo','Chloé','Nathan',
'Manon','Louis','Camille','Arthur','Sarah','Jules','Inès','Raphaël','Zoé','Paul',
'Laura','Mathis','Julie','Antoine','Marie','Thomas','Océane','Maxime','Anaïs','Victor',
'Lucie','Alexandre','Eva','Nicolas','Clara','Benjamin','Noémie','Samuel','Elodie','Julien',
'Mélanie','Kevin','Justine','Adrien','Amandine','Florian','Charlotte','Romain','Pauline','Théo',
'Sophie','Guillaume','Marion','Baptiste','Céline','Alexis','Mathilde','Corentin','Audrey','Quentin',
'Isabelle','Pierre','Hélène','Damien','Valérie','François','Sandrine','Olivier','Patricia','Laurent',
'Nathalie','Christophe','Monique','Stéphane','Véronique','Jean','Brigitte','Michel','Catherine','Daniel',
'Jacques','Sylvie','Philippe','Martine','Alain','Nicole','Bernard','Dominique','André','Christine',
'Pascal','Françoise','Thierry','Annie','Didier','Isabelle','Serge','Chantal','Yves','Nadine',
'Bruno','Corinne','Gérard','Muriel','Claude','Karine','Christian','Sabrina','Marc','Laetitia',
'Patrick','Aurélie','Frédéric','Delphine','Lionel','Émilie','Fabrice','Vanessa','Sébastien','Aurore',
'Rémi','Cindy','Mickaël','Jessica','Loïc','Morgane','Gaëtan','Marine','Jonathan','Justine',
'Anthony','Ana','Dylan','Lina','Enzo','Maya','Noah','Iris','Ethan','Lola',
'Adam','Nina','Sacha','Yasmine','Tom','Aya','Aaron','Elena','Leo','Sofia'
])[floor(random() * 200) + 1],
(ARRAY[
'Alice','Bob','Claire','David','Emma','Lucas','Léa','Hugo','Chloé','Nathan',
'Manon','Louis','Camille','Arthur','Sarah','Jules','Inès','Raphaël','Zoé','Paul',
'Laura','Mathis','Julie','Antoine','Marie','Thomas','Océane','Maxime','Anaïs','Victor',
'Lucie','Alexandre','Eva','Nicolas','Clara','Benjamin','Noémie','Samuel','Elodie','Julien',
'Mélanie','Kevin','Justine','Adrien','Amandine','Florian','Charlotte','Romain','Pauline','Théo',
'Sophie','Guillaume','Marion','Baptiste','Céline','Alexis','Mathilde','Corentin','Audrey','Quentin',
'Isabelle','Pierre','Hélène','Damien','Valérie','François','Sandrine','Olivier','Patricia','Laurent',
'Nathalie','Christophe','Monique','Stéphane','Véronique','Jean','Brigitte','Michel','Catherine','Daniel',
'Jacques','Sylvie','Philippe','Martine','Alain','Nicole','Bernard','Dominique','André','Christine',
'Pascal','Françoise','Thierry','Annie','Didier','Isabelle','Serge','Chantal','Yves','Nadine',
'Bruno','Corinne','Gérard','Muriel','Claude','Karine','Christian','Sabrina','Marc','Laetitia',
'Patrick','Aurélie','Frédéric','Delphine','Lionel','Émilie','Fabrice','Vanessa','Sébastien','Aurore',
'Rémi','Cindy','Mickaël','Jessica','Loïc','Morgane','Gaëtan','Marine','Jonathan','Justine',
'Anthony','Ana','Dylan','Lina','Enzo','Maya','Noah','Iris','Ethan','Lola',
'Adam','Nina','Sacha','Yasmine','Tom','Aya','Aaron','Elena','Leo','Sofia'
])[2 + (random()*200)::int]
FROM generate_series(1, 200000);

INSERT INTO cours(matiere)
SELECT
  n.nom || ' ' || gs.niveau
FROM
  unnest(ARRAY[
    'Mathématiques','Physique','Chimie','Biologie','Informatique','Algorithmique',
    'Bases de données','Réseaux','Systèmes','Programmation',
    'Statistiques','Probabilités','Analyse','Algèbre','Géométrie',
    'Économie','Gestion','Comptabilité','Finance','Marketing',
    'Droit','Histoire','Géographie','Philosophie','Sociologie',
    'Psychologie','Linguistique','Français','Anglais','Espagnol',
    'Allemand','Italien','Latin','Grec','Arabe',
    'Électronique','Automatique','Mécanique','Thermodynamique','Optique',
    'IA','Machine Learning','Deep Learning','Data Science','Big Data'
  ]) AS n(nom)
CROSS JOIN generate_series(1, 20) AS gs(niveau)
LIMIT 1000;

INSERT INTO inscriptions (date_insc, id_etu_fk, status)
SELECT
    NOW() - (random() * interval '365 days'),  -- date aléatoire dans l'année passée
    (1 + (random() * 199999)::int),            -- id_etu_fk aléatoire entre 1 et 5000
    (random() < 0.5)                         -- status true/false aléatoire
FROM generate_series(1,2000000);

INSERT INTO logs_connexions(id_etu_fk, id_cours_fk, date_log)
SELECT
    (1 + (random() * 199999)::int),  -- date aléatoire dans l'année passée
    (1 + (random() * 899)::int),            -- id_etu_fk aléatoire entre 1 et 5000
    NOW() - (random() * interval '365 days')                      -- status true/false aléatoire
FROM generate_series(1,5000000);