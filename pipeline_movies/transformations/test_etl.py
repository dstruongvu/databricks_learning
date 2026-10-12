import dlt
from pyspark.sql.functions import col, to_date, sum, avg, desc, count

@dlt.table(
    name="workspace.default.movies_bronze",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}
)
def movies_bronze():
  return (
    spark.readStream.table("workspace.default.movies")
  )

@dlt.table(
    name="workspace.default.movies_silver",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}
)
@dlt.expect_or_drop("valid_year_released", "year is not NULL")
def movies_silver():
  return (
    dlt.read_stream("movies_bronze")
      .withColumn("Year_Released", col("Year_Released").cast("int"))
      .withColumnRenamed("URLs", "url")
      .withColumnRenamed("Year_Released", "year")
      .withColumnRenamed("Title", "title")
      .withColumnRenamed("Cast", "cast")
      .withColumnRenamed("Plot_Summary", "plot_summary")
      .withColumnRenamed("Review", "review")
      .withColumnRenamed("Genre", "genre")
      .withColumnRenamed("Released Date", "released_date")
      .withColumnRenamed("Plot Keywords", "plot_keywords")
      .withColumnRenamed("Duration", "duration")
      .withColumnRenamed("Age of content", "age_of_content")
      .select(
          "url",
          "year",
          "title",
          "cast",
          "plot_summary",
          "review",
          "genre",
          "released_date",
          "plot_keywords",
          "duration",
          "age_of_content"
      )
  )

@dlt.table(
    name="workspace.default.movies_gold",
    table_properties={"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}
)
def movies_gold():
  return (
    dlt.read("movies_silver")
      .groupBy("year")
      .agg(
        sum("review").alias("total_review"),
        avg("review").alias("avg_review"),
        count("title").alias("total_movies")
        )
    )



