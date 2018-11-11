package com.summer.movie;

import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import scala.Tuple2;

public class MovieJoin {
public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().master("local").appName("movie").getOrCreate();
	Dataset<Row> movies = spark.read().option("inferschema", true).option("header", true)
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/movies.csv");
	//movieid genre
	Dataset<Row> idgenre = movies.flatMap((FlatMapFunction<Row,Tuple2<Integer,String>>)s->{
		ArrayList<Tuple2<Integer,String>> list = new ArrayList<>();
		for (String string : s.getString(2).split("\\|")) {
			
			list.add(new Tuple2<>(s.getInt(0),string));
		}
		return list.iterator();
		}, Encoders.tuple(Encoders.INT(), Encoders.STRING()))
			.toDF("movieId","genre");
	idgenre.repartition(functions.col("movieId")).persist();
	Dataset<Row> tags = spark.read().option("inferschema", true).option("header", true)
			.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/tags.csv");
	Dataset<Row> idtag = tags.select(functions.col("movieId"),functions.col("tag"));
	Dataset<Row> genretag = idgenre.join(idtag,"movieId");
	idgenre.unpersist();
	Dataset<Row> genrebesttag=genretag.groupBy(functions.col("genre"),functions.col("tag"))
			.count();
	genrebesttag.show(1000);
	Dataset<Row> rank=genrebesttag.withColumn("rank", functions.row_number().over(Window.partitionBy("genre").orderBy(functions.desc("count"))));
	rank.filter(functions.col("rank").lt(2)).show(100);;
	
	
}
}
