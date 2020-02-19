from datetime import timedelta
from http import HTTPStatus

import isodate
import requests
from isodate.isoerror import ISO8601Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, udf
from pyspark.sql.types import StringType

import settings
from meat_ingredients import EXPRESSIONS
from scheduler import Job, Task

spark = SparkSession.builder.master("local[*]").appName(__name__).getOrCreate()
file_name = "/tmp/recipes.json"
orc_path = "/tmp/recipes.orc"
final_orc_path = "/tmp/meat_recipes.orc"
url = settings.RECIPES_URL
expr = "|".join(EXPRESSIONS)


def parse_duration(raw_duration):
    try:
        duration = isodate.parse_duration(raw_duration)
    except ISO8601Error:
        return None
    else:
        return duration


def _complexity(prep_time, cook_time):
    with open("/tmp/log.log", 'a') as log:
        print(prep_time, cook_time, file=log)

    prep_time = parse_duration(prep_time)
    cook_time = parse_duration(cook_time)
    if not all((prep_time, cook_time)):
        return
    total = prep_time + cook_time
    if total < timedelta(minutes=30):
        return "easy"
    elif total > timedelta(hours=1):
        return "hard"
    else:
        return "medium"


complexity = udf(_complexity, StringType())


def retrieve_recipes():
    print("retrieve_recipes")
    res = requests.get(url)
    if res.status_code != HTTPStatus.OK:
        raise Exception(f"Bad status code: {res.status_code}")
    with open(file_name, "wb") as f:
        f.write(res.content)
    return file_name


def save_recipes_orc():
    print("save_recipes_orc")
    recipes = spark.read.json(file_name)
    recipes.write.format("orc").save(orc_path, mode="overwrite")


def retrieve_meat_recipes():
    recipes = spark.read.format("orc").load(orc_path)
    filtered = recipes[lower(recipes.ingredients).rlike(expr)]
    print(filtered.count())
    filtered.withColumn("complexity", complexity("prepTime", "cookTime"))\
        .write.format("orc").save(final_orc_path)


job = Job()
retrieve_recipes_task = Task(retrieve_recipes, "retrieve_recipes", job)
save_recipes_orc_task = Task(save_recipes_orc, "save_recipes_orc", job)
retrieve_meat_recipes_task = Task(retrieve_meat_recipes, "retrieve_meat_recipes", job)


retrieve_recipes_task.set_upstream(save_recipes_orc_task)
save_recipes_orc_task.set_upstream(retrieve_meat_recipes_task)


if __name__ == "__main__":
    job.run()
