package com.summer.movie;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;

public class MovieRecommendation {
public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().master("local[*]").appName("movie").getOrCreate();
	Dataset<Row> movies = spark.read().option("inferschema", true).option("header", true)
				.option("delimiter", ",")
				.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-20m/rating4.csv")
				.drop(functions.col("timestamp"));
	movies.persist();
	movies.printSchema();
	Dataset<Row> raters = movies.groupBy(functions.col("movieId")).agg(functions.count("rating").as("count"));
	//userId,movieId,ratings,numraters
	Dataset<Row> data = movies.join(raters,"movieId");
	
	data.persist();
	movies.unpersist();
	//dupdata for self join
	Dataset<Row> dupdata =data.toDF("movieId2","userId","rating2","count2");
	//self joined data
	Dataset<Row> selfjoined = data.join(dupdata,"userId");
	// filter to exclude pairs (A,B) (B,A) and self pairs(A,A)
	Dataset<Row> filterdata = selfjoined.filter(functions.col("movieId").lt(functions.col("movieId2")));
	data.unpersist();
	//filterdata.show(1000);
	// renaming dup col as (xxx)2 and genrating xy,x2,y2
	Dataset<Row> caldata = filterdata
			.withColumn("r1r2",functions.col("rating").multiply(functions.col("rating2")))
			.withColumn("sqrating1",functions.col("rating").multiply(functions.col("rating")))
			.withColumn("sqrating2",functions.col("rating2").multiply(functions.col("rating2")))
			;
	//caldata.show();
	// calculating sum(x2), sum(y2), sum(xy),sum(x),sum(y)
	Dataset<Row> caldata2=caldata.groupBy("movieId","movieId2")
			.agg(functions.sum("r1r2").as("r1r2S"),functions.sum("rating").as("ratingS"),
			functions.sum("rating2").as("rating2S"),functions.sum("sqrating1").as("sqrating1S")
			,functions.sum("sqrating2").as("sqrating2S"),functions.count("rating").as("count1")
			,functions.count("rating2").as("n"));
	caldata2.persist();
	caldata2.show(false);
	//size of variables
	long n = caldata2.count();
	String nr = "("+"n"+"*(r1r2S)) - ((ratingS*rating2S))";
	String dr = "(("+"n"+"*sqrating1S)-(ratingS*ratingS))"+"*(("+"n"+"*sqrating2S)-(rating2S*rating2S))";
	Dataset<Row> finaldatat = caldata2.withColumn("corelation",functions.expr(nr)
			.divide(functions.sqrt(functions.expr(dr))));
	caldata2.unpersist();
	 
	Dataset<Row> finaldata = finaldatat.withColumn("rc",
			functions.col("corelation").multiply(functions.col("n").divide(functions.col("n").plus(50))));
	
	
	Dataset<Row> movielink =spark.read().option("inferschema", true)
			.option("header", false)
			.option("delimiter", "|")
			.csv("/Users/rushikesh/Desktop/spark-aadhar/movie/ml-100k/movies")
			.select(functions.col("_c0"),functions.col("_c1")).toDF("movieId","title")
			;
	movielink.printSchema();
	movielink.persist();
	Dataset<Row> relateddata= finaldata.join(movielink,"movieId").withColumnRenamed("title", "title1");
	

	Dataset<Row> relateddata2=relateddata
			.join(movielink,relateddata.col("movieId2").equalTo(movielink.col("movieId")))
			.withColumnRenamed("title", "title2");
	movielink.unpersist();
	relateddata2.select(functions.col("title1"),functions.col("title2"),functions.col("corelation"),functions.col("rc"))
	.filter(functions.col("title1").contains("Mrs. Doubtfire (1993)")).sort(functions.desc("rc")).show(3000,false);
	
	
	
	
	

}
}
