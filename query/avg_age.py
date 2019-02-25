
from pyspark.sql.types import IntegerType
from sqlalchemy import text

def py_mysql(engine):
    
    query = """
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
    """
    stmt = text(query)
    
    conn = engine.connect()
    
    rs = conn.execute(stmt)
    
    rs.fetchall()
    
    conn.close()
    
def spark_mysql(name_basics, title_basics, title_principals):
    names = name_basics.select('nconst', 'birthYear')
    names = names.where(names['birthYear'].isNotNull())
    
    titles = title_basics.select('tconst', 'startYear')
    titles = (titles.withColumn('startYear', titles['startYear'].cast(IntegerType()))
                     .where(titles['startYear'].isNotNull()))
    
    principals = title_principals.select('tconst', 'category', 'nconst')
    principals = (principals.where(principals['category'].isin(['actor', 'actress'])))
    
    result = principals.join(titles, on=['tconst']).join(names, on=['nconst'])
    
    result = result.withColumn('age', result['startYear'] - result['birthYear'])
    
    result = result.groupBy(['startYear', 'category']).avg('age')
    result = result.orderBy(['startYear', 'category'])
    
    return result

pipeline = [
        { "$match": {"birthYear": {"$ne": '\\N'} } },
        { "$lookup": {
                "from": "principals",
                "localField": "nconst",
                "foreignField": "nconst",
                "as": "principal"
            }
        },
        { "$unwind": "$principal" },
        { "$match": {"principal.category" : {"$in" : ["actress","actor"]}} },
        { "$lookup": {
                "from": "titles",
                "localField": "principal.tconst",
                "foreignField": "tconst",
                "as": "title"
            }
        },
        { "$unwind": "$title" },
        { "$match": {"title.startYear": {"$ne": '\\N'} } },
        { '$addFields': { 'age': {"$subtract" :["$title.startYear", "$birthYear"]}}},
        { '$group':{
             '_id': {'year': "$title.startYear", 'category': '$principal.category'},
             'avg_age': {"$avg": "$age"}
        }},
        {"$sort": {"_id.year": 1, "_id.category": 1} },
        { "$project": {
            "_id": 1,
            'avg_age': 1
            }
        }
    ]

def py_mongo(mongo_client, pipeline):
    db = mongo_client['imdb']
    
    db.people.aggregate(pipeline)
    #pprint.pprint(db.command('aggregate', 'titles', pipeline=pipeline, explain=True))
    
def spark_mongo(spark, pipeline):
    
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", "mongodb://localhost:27017/imdb.people") \
            .option("pipeline", pipeline) \
            .load()
    return df