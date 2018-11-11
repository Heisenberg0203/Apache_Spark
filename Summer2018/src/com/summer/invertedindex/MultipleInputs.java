package com.summer.invertedindex;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import scala.*;

public class MultipleInputs {
public static void main(String[] args) {
	SparkSession spark =SparkSession.builder().master("local[*]").appName("InvertedIndex").getOrCreate();
	

	
	SparkContext sc = spark.sparkContext();
	RDD<Tuple2<String, String>> input=sc.wholeTextFiles("/Users/rushikesh/Desktop/spark-aadhar/inverted-index/dataset/shakespeare",1);
	
	JavaRDD<Tuple2<String, String>> input2 = input.toJavaRDD();
	//Dataset<Tuple2<String, String>> h = spark.createDataset(input,Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
	
	
	JavaRDD<Tuple3<String, String,Integer>> words = input2.flatMap(s->{
		ArrayList<Tuple3<String,String,Integer>> wordpath = new ArrayList<>();
		String temptext = s._2;
		String text =temptext.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ").toLowerCase();
		for (String string : text.split(" ")) {
			wordpath.add(new Tuple3<>(string,s._1,1));
		}
		return wordpath.iterator();
	});
	
	JavaPairRDD<String, Integer> keyvalue= words.mapToPair(s->new Tuple2<>(s._1()+"#"+s._2(),s._3()));
	JavaPairRDD<String,Integer> countwords = keyvalue.reduceByKey((x,y)->(x+y));
	JavaPairRDD<String,String> output = countwords.mapToPair(s->new Tuple2<>(s._1.split("#")[0],s._1.split("#")[1]+"\t"+s._2));
	
	JavaPairRDD<String,Iterable<String>> InvertedIndex = output.groupByKey();
			//.mapToPair(s->new Tuple2<>(s._1,StringUtils.join(iterableToCol, "")));
	InvertedIndex.saveAsTextFile("/Users/rushikesh/Desktop/spark-aadhar/inverted-index/dataset/shakespeare/output2");
}
}
