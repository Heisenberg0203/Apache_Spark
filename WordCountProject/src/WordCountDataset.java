import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

import scala.Function2;
import scala.Tuple2;

public class WordCountDataset {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("wordcount").master("local").getOrCreate();

		Dataset<String> input = spark.read().textFile("/Users/rushikesh/Desktop/javaprac/practicals/calculator.txt");
		//dataset
		Dataset<String> words = input.flatMap(
				(FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

		Dataset<Row> wordCount = words.groupBy(words.col("value")).count().sort(functions.desc("count"));
		//wordCount.show();
		//dataframe
		Dataset<Row> df = input.toDF();
		Dataset<String> wordsDf = df.flatMap(
				(FlatMapFunction<Row, String>) s -> Arrays.asList(s.toString().split(" ")).iterator(),
				Encoders.STRING());
		Dataset<Row> wordCountDf = wordsDf.groupBy(wordsDf.col("value")).count().sort(functions.desc("count"));
		//wordCountDf.show();
		
		// sql
		words.createOrReplaceTempView("wordsandOnes");
		Dataset<Row> sqlcount = spark.sql("select *,count(*) as count from wordsandOnes group by value sort by count desc   ");
		//sqlcount.show();
		//
		 Dataset<Tuple2<String,Integer>> wordsAndOnes = words.map(
		 (MapFunction<String,Tuple2<String,Integer>>) s-> new
		 Tuple2<>(s,1),Encoders.tuple(Encoders.STRING(), Encoders.INT()));
		
		wordsAndOnes.toDF("value","one").groupBy("value").agg(functions.sum("one")).show();;

	}
}
