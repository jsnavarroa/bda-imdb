SELECT 
   distinct category
FROM
    imdb.title_principals;


SELECT 
   *
FROM
    imdb.title_principals tp 
    join imdb.title_crew_writer writer on tp.tconst=writer.tconst
where tp.nconst = writer.writer
order by tp.tconst;

/* Average actor and actress age by year*/
SELECT 
    title.startYear,
    principal.category,
    AVG(title.startYear - person.birthYear) AS avg_age
FROM
    imdb.title_principals principal
        JOIN
    imdb.title_basics title ON title.tconst = principal.tconst
        JOIN
    imdb.name_basics person ON principal.nconst = person.nconst
WHERE
    person.birthYear IS NOT NULL
    and title.startYear IS NOT NULL
    and principal.category in ('actor', 'actress')
GROUP BY title.startYear, principal.category
order by title.startYear, principal.category ;

