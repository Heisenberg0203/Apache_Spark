package com.summer.movie;

import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import org.apache.spark.sql.SparkSession;

import com.esotericsoftware.kryo.io.Input;

import scala.*;

public class MovieAnalysis {
	public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().master("local[*]").appName("movie").getOrCreate();
	Dataset<Row> movies = spark.read().option("inferschema", true).option("header", true)
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/movies.csv");
	movies.printSchema();
	Dataset<Row> movieyear = movies.select("title")
			.map((MapFunction<Row,Tuple2<String,Integer>>)s->{
				String year = s.toString().replaceAll("[^0-9]","");
				return new Tuple2<>(year,1);
			}, Encoders.tuple(Encoders.STRING(), Encoders.INT()))
			.toDF("year","count");
	
	movieyear.filter(functions.col("year").isNotNull())
	.groupBy(functions.col("year")).agg(functions.sum("count").as("totalcount"))
	.sort(functions.desc("totalcount")).show();
	

	
//	Dataset<Row> yeargenre = movies.flatMap((FlatMapFunction<Row,Tuple2<String,String>>)s->{
//		ArrayList<Tuple2<String,String>> list = new ArrayList<>();
//		for (String string : s.getString(2).split("\\|")) {
//			if(s.getString(1).replaceAll("[^0-9]","").length()==4)
//			list.add(new Tuple2<>(s.getString(1).replaceAll("[^0-9]",""),string));
//		}
//		return list.iterator();
//		}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
//			.toDF("year","genre");
//	
//	yeargenre.groupBy(functions.col("year"),functions.col("genre")).count()
//	.sort(functions.desc("year"),functions.desc("count")).show();
	
	
	
	
	
	}
	
	
	
	
	
	

}
