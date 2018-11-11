package com.summer.twiiter;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SentimentAnalysisDS {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("sentiment").master("local").getOrCreate();
		Dataset<Row> input = spark.read().option("inferschema", true).json("/Users/rushikesh/Desktop/tweet.json");
		Dataset<Row> idtext = input.select(input.col("text"),input.col("id"));
		//idtext.show();
		
		//id,words
		Dataset<Tuple2<String,String>> idwords = idtext.flatMap((FlatMapFunction<Row,Tuple2<String,String>>)s->{
		ArrayList<Tuple2<String,String>> idword = new ArrayList<>();
		for (String string : s.getString(0).split(" ")) {
			idword.add(new Tuple2<>(s.getString(1),string));
		}
		return idword.iterator();
		}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
		Dataset<Row> idworddf=idwords.toDF("id","word");
		
		Dataset<Row> affin = spark.read().option("inferschema", true).option("delimiter","\t").csv("/Users/rushikesh/Desktop/AFINN.txt");
		
		Dataset<Row> idrate = idworddf.as("a").join(affin.as("b"),idworddf.col("word").equalTo(affin.col("_c0")));
		Dataset<Row> tweets=idrate.groupBy("id").agg(functions.sum("_c1").as("rate"));
		Dataset<Row> postweet = tweets.filter(functions.col("rate").gt(0)).sort(functions.desc("rate"));
		postweet.show(100);
		
		
	}

}
