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

/* Best 10 titles per year */
SELECT * FROM (
SELECT
	ROW_NUMBER() OVER (PARTITION BY titles.titleType, titles.startYear ORDER BY averageRating DESC) AS row_num,
    originalTitle,
    titleType,
    startYear, 
    averageRating
FROM
    title_basics titles
        JOIN
    title_ratings ratings ON titles.tconst = ratings.tconst
WHERE startYear IS NOT NULL
) AS ranking
WHERE ranking.row_num <= 10
ORDER BY titleType, startYear, averageRating DESC
;

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
	AND title.startYear IS NOT NULL
	AND principal.category in ('actor', 'actress')
GROUP BY title.startYear, principal.category
ORDER BY title.startYear, principal.category ;

