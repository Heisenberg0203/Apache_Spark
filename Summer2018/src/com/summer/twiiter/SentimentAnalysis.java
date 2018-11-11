package com.summer.twiiter;

import java.util.ArrayList;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SentimentAnalysis {
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().appName("sentiment").master("local").getOrCreate();
		
		JavaRDD<Row> input = spark.read().option("inferschema", true).json("/Users/rushikesh/Desktop/tweet.json").toJavaRDD();
		System.out.println(input.first());
		JavaRDD<String> idtext = input.map(s->{
								String[] tokens=s.toString().split(",");
								return (tokens[1]+"\t\t"+tokens[3]);
								});
		// id words
		JavaPairRDD<String,String> rawwords = idtext.flatMapToPair(s->{
			ArrayList<Tuple2<String,String>> idwords = new ArrayList<>();
			String[] tokens = s.split("\t\t");
			for (String string : tokens[1].split(" ")) {
				//string =word
				idwords.add(new Tuple2<>(string,tokens[0]));
				}
		return idwords.iterator();
		}).partitionBy(new HashPartitioner(20));
		idtext.persist(StorageLevel.MEMORY_ONLY());
		//words rating	
		JavaRDD<String> rateinput = spark.read().textFile("/Users/rushikesh/Desktop/AFINN.txt").toJavaRDD();
		JavaPairRDD<String,Integer> ratewords=rateinput.flatMapToPair(s->{
			ArrayList<Tuple2<String,Integer>> idwords = new ArrayList<>();
			String[] tokens = s.split("\t");
			//tokens[0] =word
				idwords.add(new Tuple2<>(tokens[0],Integer.parseInt(tokens[1])));
				
		return idwords.iterator();
		});
		// word,id,rateing
		JavaPairRDD<String,Tuple2<String,Integer>> rawjoindata= rawwords.join(ratewords);
		//id,rating
		JavaPairRDD<String,Integer> result = rawjoindata.mapToPair(s->(new Tuple2<String, Integer>(s._2._1,s._2._2)));
		//id,score
		JavaPairRDD<String,Integer> finalresult= result.reduceByKey((x,y)->(x+y));
		
		finalresult.filter(s->s._2>0).mapToPair(s->s.swap()).sortByKey().mapToPair(s->s.swap()).coalesce(1).saveAsTextFile("/Users/rushikesh/Desktop/spark-aadhar/sentiment/outputwithpartition");
		
		
		}
}
