package com.summer.twiiter;

import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple3;

public class SentimentAnalysisHashTag {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("sentiment").master("local").getOrCreate();
		Dataset<Row> input = spark.read().option("inferschema", true).json("/Users/rushikesh/Desktop/tweet.json");
		Dataset<Row> idtext = input.select(input.col("text"),input.col("id"))
				.filter(input.col("text").contains("#music"));
		//idtext.show();
		
		//id,text,words
		Dataset<Tuple3<String,String,String>> idwords = idtext.flatMap((FlatMapFunction<Row,Tuple3<String,String,String>>)s->{
		ArrayList<Tuple3<String,String,String>> idword = new ArrayList<>();
		for (String string : s.getString(0).split(" ")) {
			idword.add(new Tuple3<>(s.getString(1),s.getString(0),string));
		}
		return idword.iterator();
		}, Encoders.tuple(Encoders.STRING(), Encoders.STRING(),Encoders.STRING()));
		Dataset<Row> idworddf=idwords.toDF("id","text","word");
		
		Dataset<Row> affin = spark.read().option("inferschema", true).option("delimiter","\t").csv("/Users/rushikesh/Desktop/AFINN.txt");
		
		Dataset<Row> idrate = idworddf.as("a").join(affin.as("b"),idworddf.col("word").equalTo(affin.col("_c0")));
		Dataset<Row> tweets=idrate.groupBy("id","text").agg(functions.sum("_c1").as("rate"));
		Dataset<Row> postweet = tweets.filter(functions.col("rate").gt(0)).sort(functions.desc("rate"));
		postweet.show(100,false);
	}

}
