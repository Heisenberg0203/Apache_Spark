package com.summer.movie;

import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import scala.Tuple3;

public class BestGenreDecade {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local").appName("movie").getOrCreate();
		
		Dataset<Row> moviestemp = spark.read().option("inferschema", true).option("header", true)
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/movies.csv");
		
		Dataset<Row> ratingstemp = spark.read().option("inferschema", true).option("header", true)
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/ratings.csv");
		
		// movieId, rating
		Dataset<Row> ratings = ratingstemp.select(functions.col("movieId"), functions.col("rating"));
		ratings.repartition(1000).persist();
		
		// movieId,title+year,genre->movieId,genre,year
		Dataset<Row> movies = moviestemp.flatMap((FlatMapFunction<Row, Tuple3<Integer, String, String>>) s -> {
			String year;
			ArrayList<Tuple3<Integer, String, String>> list = new ArrayList<>();
			for (String string : s.getString(2).split("\\|")) {
				if (s.getString(1).substring(0, 1).equals("\"")) {
					year = s.getString(1).substring(s.getString(1).length() - 6, s.getString(1).length() - 2);
				} else {
					year = s.getString(1).substring(s.getString(1).length() - 5, s.getString(1).length() - 1);
				}
				list.add(new Tuple3<>(s.getInt(0), string, year));
			}
			return list.iterator();
		}, Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.STRING())).toDF("movieId", "genre", "year");

		String yr = "((year/10))";
		Dataset<Row> moviesf = movies.withColumn("year", functions.floor(functions.expr(yr)).multiply(10));

		// movieid,genre,rating,decade
		Dataset<Row> data = ratings.join(moviesf, "movieId");
		ratings.unpersist();
		
		Dataset<Row> grpdata = data.groupBy("year", "genre")
				.agg(functions.count("rating").as("count"), functions.avg("rating").as("mean"))
				.sort(functions.desc("mean"));
		// usual ratings
		String avgs = data.agg(functions.mean("rating")).as(Encoders.STRING()).first().toString();
		// mean(ratings) as avg in double
		Double avg = Double.parseDouble(avgs);
		// data.select(functions.avg("rating")).toString();
		// min iter is 500
		String expr2 = "(count/(count+500))*mean";
		String expr3 = "(500/(500+count))";

		// weighted rating
		Dataset<Row> wrating = grpdata.withColumn("wr",
				functions.expr(expr2).plus((functions.expr(expr3).multiply(avg))));
		// wrating.sort(functions.desc("wr")).show();
		Dataset<Row> decadebest = wrating.withColumn("rank",
				functions.row_number().over(Window.partitionBy("genre").orderBy(functions.desc("wr"))));
		// searching for any one genre
		String fgenre = "Action";
		decadebest.filter(functions.col("genre").equalTo(fgenre)).sort(functions.asc("rank")).show(100, false);

	}
}
