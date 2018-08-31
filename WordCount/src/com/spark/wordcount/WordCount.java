package com.spark.wordcount;


import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.*;


import scala.Tuple2;




public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Starterpack");
		conf.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> input = sc.textFile("/Users/rushikesh/Desktop/javaprac/practicals/calculator.txt");
		// lines to words
		JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
			// TODO Auto-generated method stub
			
		
		
		//words and its count
//		JavaPairRDD<String, Integer> wordOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
//			@Override
//			public Tuple2<String, Integer> call(String word) throws Exception {
//				return new Tuple2(word,1);
//			}});
		JavaPairRDD<String, Integer> wordOnes = words.mapToPair(word -> new Tuple2<>(word,1));
		
		//reducebyKey
//		JavaPairRDD<String, Integer> wordcount = wordOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
//			
//			@Override
//			public Integer call(Integer x, Integer y) throws Exception {
//				// TODO Auto-generated method stub
//				return (x+y);
//			}});
		JavaPairRDD<String, Integer> wordcount = wordOnes.reduceByKey((x, y) -> (x+y));
		//saving the output
		wordcount.saveAsTextFile("/Users/rushikesh/Documents/Hadoopmain/sparkjars/outputwc");
	
		sc.close();
	}

}
