
from pyspark.sql.functions import col, desc, rank
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from sqlalchemy import text

def py_mysql(engine):
    
    query = """
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
        ORDER BY titleType, startYear, averageRating DESC;
    """
    stmt = text(query)
    
    conn = engine.connect()
    
    rs = conn.execute(stmt)
    
    rs.fetchall()
    
    conn.close()
    
def spark_mysql(title_basics, title_ratings):
    
    titles = title_basics.select('tconst', 'startYear', 'originalTitle', 'titleType')
    titles = titles.withColumn('startYear', titles['startYear'].cast(IntegerType()))\
                    .where(titles['startYear'].isNotNull())
    
    ratings = title_ratings.select('tconst', 'averageRating')
    
    result = ratings.join(titles, on=['tconst'])
    
    window = Window.partitionBy(['titleType', 'startYear']).orderBy(desc('averageRating'))

    result = result.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10)
    
    result = result.orderBy('titleType', 'startYear', desc('averageRating'))
    
    return result