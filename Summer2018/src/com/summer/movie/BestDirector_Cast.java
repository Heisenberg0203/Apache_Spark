package com.summer.movie;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

public class BestDirector_Cast {
public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().master("local").appName("movie").getOrCreate();
	
	Dataset<Row> webdata = spark.read().option("inferschema", true).option("header", true)
			.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/imdb_df.csv");
	//id,director's array
	//select cast/director and explode respectivetly
	Dataset<Row> directors = webdata.select(functions.col("movieId"),functions.col("cast"))
			.withColumn("director", functions.split(functions.col("cast"),"\\|"));
	
	//id,director
	Dataset<Row> director = directors.withColumn("director",
			functions.explode(functions.col("director")));
	
	Dataset<Row> ratingstemp = spark.read().option("inferschema", true).option("header", true)
			.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/ratings.csv");
	//id,ratings
	Dataset<Row> ratings = ratingstemp.select(functions.col("movieId"),functions.col("rating"));
	ratings.repartition(1000).persist();
	//director,ratings
	Dataset<Row> dirrating =ratings.join(director,"movieId")
			.select(functions.col("director"),functions.col("rating"));
	ratings.unpersist();
	Dataset<Row> grpdata = dirrating.groupBy(functions.col("director"))
			.agg(functions.count("rating").as("count"),functions.mean("rating").as("meanrating"));
	grpdata.persist();
	String avgs = grpdata.agg(functions.mean("meanrating")).as(Encoders.STRING()).first().toString();
	// mean(ratings) as avg in double
	Double meanwhole = Double.parseDouble(avgs);
	// data.select(functions.avg("rating")).toString();
	// min iter is 500
	String expr2 = "(count/(count+500))*meanrating";
	String expr3 = "(500/(500+count))";
	
	// weighted rating
	Dataset<Row> wrating = grpdata.withColumn("wr",
					functions.expr(expr2).plus((functions.expr(expr3).multiply(meanwhole))));
	// wrating.sort(functions.desc("wr")).show();
	wrating.sort(functions.desc("wr")).show(false);
	
					
}
}
