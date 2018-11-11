package com.summer.secondarysort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import scala.Tuple2;
public class SortMultipleFiles {
public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().appName("sort").master("local").getOrCreate();
	
	Dataset<Row> input = spark.read()
			.option("inferschema", true)
			.option("header", false)
			.csv("/Users/rushikesh/Desktop/spark-aadhar/secondarysort/dataset/Names.csv");

	//System.out.println(input.sort(input.col("_c0"),input.col("_c1")).toJavaRDD().toDebugString());
//	//always coalesce when u need one file /sort/colaesce/partition
//	//input.sort(input.col("_c0"),input.col("_c1")).coalesce(1).write().partitionBy("_c0").text("/Users/rushikesh/Desktop/spark-aadhar/secondarysort/output2");
//
//	//repartioon/sort	
	Dataset<Row>h=input.repartition(input.col("_c0")).sortWithinPartitions(input.col("_c1"));
	
	//h.write().csv("/Users/rushikesh/Desktop/spark-aadhar/secondarysort/output33");
	h.toJavaRDD().saveAsTextFile("/Users/rushikesh/Desktop/spark-aadhar/secondarysort/output34");
	
				
}
}
