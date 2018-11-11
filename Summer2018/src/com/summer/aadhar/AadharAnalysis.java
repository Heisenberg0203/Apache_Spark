package com.summer.aadhar;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

public class AadharAnalysis {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Aadhaar").master("local").getOrCreate();
		Dataset<Row> input = spark.read().option("header", true).option("inferschema",true).csv("/Users/rushikesh/Desktop/spark-aadhar/aadhaar-dataset-analysis/data/UIDAI-ENR-DETAIL-20170308.csv");
		input.printSchema();
		
		//identities generated in each state
		Dataset<Row> stateCount= input.groupBy(input.col("State")).count();
		//stateCount.show();
		//identities generated in each agency
		Dataset<Row> agencyCount= input.groupBy(input.col("Enrolment Agency")).count();
		//agencyCount.show();
		
		// male and female count
		Dataset<Row> mCount = input.filter(input.col("Gender").equalTo("M")).groupBy(input.col("District")).count().as("mcount");
		Dataset<Row> fCount = input.filter(input.col("Gender").equalTo("F")).groupBy(input.col("District")).count().as("fcount");
		Dataset<Row> mfcount = mCount.join(fCount,"District");
		String expr="mcount.count+fcount.count";

		//mfcount.sort(functions.expr(expr).desc()).show();
		
		//unique pincodes
		long l=input.select(input.col("Pin Code")).distinct().count();
		input.agg(functions.countDistinct("Pin Code")).show();
		//System.out.println(l);
		
		//% for male is high
		// partition the large dataset over the column you want to join 
		Dataset<Row> data = input.select(input.col("State"),input.col("District"),input.col("Gender"),input.col("Aadhaar generated"),input.col("Enrolment Rejected"));
		data.repartition(data.col("State")).persist();
		//max count of male
		long mc = data.select(data.col("Aadhaar generated")).filter(data.col("Aadhaar generated").equalTo(1)).count();
		//male perecentage
		Dataset<Row> male =data.filter(data.col("Gender").equalTo("M"))
				.groupBy(data.col("State")).agg(functions.sum("Aadhaar generated").divide(mc).multiply(100).as("%"));
		// top 3 states having male percentage
		Dataset<Row> maletop= male.sort(functions.desc("%")).limit(3);
		maletop.persist();
		//maletop.show();
		//female data from top 3 states having high % of male
		Dataset<Row> femaledata = data.as("a").join(maletop.as("b"),"State").filter(data.col("Gender").equalTo("F"));
		data.unpersist();
		maletop.unpersist();
		//max count of female
		long fc=femaledata.select(femaledata.col("Aadhaar generated")).filter(femaledata.col("Aadhaar generated").equalTo(1)).count();
		//female percentage 
		Dataset<Row> femalepercent = femaledata.groupBy("State","District").agg(functions.sum("Aadhaar generated").divide(fc).multiply(100).as("f%"));
		
		//top 3 districts from each state(3 states from above)
		//femalepercent.withColumn("rank", functions.row_number().over(Window.partitionBy("State").orderBy(functions.desc("f%")))).filter(functions.col("rank").lt(4)).show();
		
		//enrollment rejected gender wise in a district
		Dataset<Row> rejecteddata = input.select(input.col("District"),input.col("Gender"),
									input.col("Enrolment Rejected"))
				.filter(input.col("Enrolment Rejected").equalTo(1));
		Dataset<Row> dstrejecteddata=rejecteddata.groupBy("District","Gender").count();
		
		Dataset<Row> rankwiserejected=dstrejecteddata.withColumn("rank", functions.row_number()
				.over(Window.partitionBy("Gender").orderBy(functions.desc("count"))));
		
		Dataset<Row> toprejected = rankwiserejected.filter(functions.col("rank").lt(4));
		//toprejected.show();
	

	}

}
