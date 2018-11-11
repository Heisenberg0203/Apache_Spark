package com.summer.movie;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import scala.Tuple3;

public class BestMovieDecade {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local").appName("movie").getOrCreate();
		Dataset<Row> moviestemp = spark.read().option("inferschema", true).option("header", true)
					.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/movies.csv");
		Dataset<Row> ratingstemp = spark.read().option("inferschema", true).option("header", true)
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/ratings.csv");
		//movieId, rating
		Dataset<Row> ratings = ratingstemp.select(functions.col("movieId"),functions.col("rating"));
		ratings.repartition(1000).persist();
		//movieId,movie,year
		//look behind nondigit and look forward digit| look behind digit  and look forward non digit
		Dataset<Row> movies =moviestemp.map((MapFunction<Row,Tuple3<Integer,String,String>>)s->{
			String title,year;
			if(s.getString(1).substring(0,1).equals("\"")) {
				
			 title = s.getString(1).substring(0,s.getString(1).length()-7);
			 year = s.getString(1).substring(s.getString(1).length()-6, s.getString(1).length()-2);}
			else {
				 title = s.getString(1).substring(0,s.getString(1).length()-6);
			 year = s.getString(1).substring(s.getString(1).length()-5, s.getString(1).length()-1);
			}
			
			return new Tuple3<>(s.getInt(0),title,year);}
		, 
				Encoders.tuple(Encoders.INT(),Encoders.STRING(), Encoders.STRING())).toDF("movieId","title","year");
		
		String yr = "((year/10))";
		Dataset<Row> moviesf = movies.withColumn("year",functions.floor(functions.expr(yr)).multiply(10));
	
		//movieid,name,rating,decade
		Dataset<Row> data = ratings.join(moviesf,"movieId");
		ratings.unpersist();
		Dataset<Row> grpdata=data.groupBy("year","movieId","title")
		.agg(functions.count("rating").as("count"),functions.avg("rating").as("mean")).sort(functions.desc("mean"));
		//usual ratings
		String avgs = data.agg(functions.mean("rating")).as(Encoders.STRING()).first().toString();
		//mean(ratings) as avg in double
 		Double avg = Double.parseDouble(avgs);
				//data.select(functions.avg("rating")).toString();
		//min iter is 500
		String expr2="(count/(count+500))*mean";
		String expr3="(500/(500+count))";
		
		//weighted rating
		Dataset<Row> wrating = grpdata.withColumn("wr",functions.expr(expr2).plus((functions.expr(expr3).multiply(avg))));
		//sorted data
		//wrating.sort(functions.desc("wr")).show();
		Dataset<Row> decadebest = wrating.withColumn("rank",functions.row_number().over(
				Window.partitionBy("year").orderBy(functions.desc("wr"))));
		// getting top one
		decadebest.filter(functions.col("rank").lt(2)).sort(functions.desc("year")).show(100,false);
		
		
	} 
	

}
