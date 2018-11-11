package com.summer.freinds;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.spark_project.dmg.pmml.Aggregate.Function;

import com.google.common.base.Functions;

import scala.Tuple2;
import scala.Tuple3;

public class FreindRecommendation {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("movie").getOrCreate();
		Dataset<Row> input = spark.read().option("inferschema", true)
				.option("header", false)
					.option("delimiter", " ")
					.csv("/Users/rushikesh/Desktop/spark-aadhar/facebook/1912.edges")
					.toDF("f1","f2");
		input.printSchema();
		//input.show();
		Dataset<Row>grpdata=input.groupBy("f1").agg(functions.collect_list("f2").as("list"));
		//grpdata.show(false);
		
		Dataset<Row> data = grpdata.flatMap((FlatMapFunction<Row,Tuple3<String,String,Integer>>)s->{
			ArrayList<Tuple3<String,String,Integer>> mlist = new ArrayList<>();
			List<Object> list =  s.getList(1);
			ArrayList<String> flist = new ArrayList<>();
			for (Object friend : list) {
				flist.add(friend.toString());
			}
			//direct connectins
			for (String string : flist) {
				if(s.get(0).toString().compareTo(string)>0)
				mlist.add(new Tuple3<>(s.get(0).toString(),string,0));
				else if(s.get(0).toString().compareTo(string)<0)
					mlist.add(new Tuple3<>(string,s.get(0).toString(),0));	
			}
			//future connections
			for (String string : flist) {
				for (String string2 : flist) {
					if(string2.compareTo(string)>0)
						mlist.add(new Tuple3<>(string2,string,1));
						
				}
			}
			return mlist.iterator();
		}, Encoders.tuple(Encoders.STRING(),Encoders.STRING(),Encoders.INT())).toDF("f1","f2","link");
		
		Dataset<Row> result = data.groupBy("f1","f2").agg(functions.sum("link").as("sum"),functions.count("link").as("count"));
		result.withColumn("mrating", functions.col("sum").divide(functions.col("count"))).sort(functions.desc("mrating"),functions.desc("count")).show(3000);
 	}
}
