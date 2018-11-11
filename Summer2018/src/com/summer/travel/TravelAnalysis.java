package com.summer.travel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.spark_project.dmg.pmml.Aggregate.Function;
import org.apache.spark.sql.SparkSession;

public class TravelAnalysis {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().
				appName("travelAnalysis").
				master("local").
				getOrCreate();

		Dataset<Row> input = spark.read().format("csv").
				option("delimiter", "\t").
				option("inferschema", true)
				.load("/Users/rushikesh/Desktop/spark-aadhar/travel analysis/TravelData.txt");
		input.printSchema();
		// prior questions
		
		// 20 destination
		Dataset<Row> destination = input.select(input.col("_c2")).
				groupBy("_c2").count().
				sort(functions.desc("count"))
				.limit(20);
		//destination.show();

		// 20 source
		Dataset<Row> source = input.select(input.col("_c1"))
				.groupBy("_c1").count()
				.sort(functions.desc("count"))
				.limit(20);
		//source.show();

		// 20 cities(Source ) that generates high revenue for air

		Dataset<Row> aircities = input.select(input.col("_c1"), input.col("_c12"),input.col("_c4"))
				.filter(input.col("_c4")
				.equalTo(1).or(input.col("_c4").equalTo(3)).or(input.col("_c4").equalTo(7)));
		

		Dataset<Row> topAirFare = aircities.groupBy("_c1").
				agg(functions.sum("_c12").as("sum"))
				.sort(functions.desc("sum"))
				.limit(20);
		//topAirFare.show();
		
		// age group choosing mode of travel and destination
			Dataset<Row> adultData = input.select(input.col("_c4"),input.col("_c2"),input.col("_c3"))
									.filter(input.col("_c4").equalTo(1));
			// same can be done for any age group
				//destn for adult
				Dataset<Row> adultDestn = adultData.groupBy(adultData.col("_c2")).count().sort(functions.desc("count")).limit(20);
				//adultDestn.show();
				//mode for adult
				Dataset<Row> adultMode = adultData.groupBy(adultData.col("_c3")).count().sort(functions.desc("count")).limit(20);
				//adultMode.show();
				// mode and destination for adult
				Dataset<Row> adultCombo = adultData.groupBy(adultData.col("_c2"),adultData.col("_c3")).count().sort(functions.desc("count")).limit(20);
				//adultCombo.show();
				
		// hotels preffered
				Dataset<Row> hotelData = input.select(input.col("_c17"),input.col("_c13"))
						;
				hotelData.persist();
				// count of hotel
				Dataset<Row> hotelCount = hotelData.filter(hotelData.col("_c17").isNotNull())
						.groupBy(hotelData.col("_c17")).count();
				//rank acording to hotel popularity
				Dataset<Row> hotelCountRank = hotelCount.withColumn("rank", functions.row_number()
						.over(Window.orderBy(functions.desc("count"))));
				
				//price wise ranking
				Dataset<Row> hotelPrice = hotelData.filter(hotelData.col("_c17").isNotNull())
								.groupBy(hotelData.col("_c17")).agg(functions.avg("_c13").as("avgprice"));
				
				Dataset<Row> hotelPriceRank = hotelPrice.withColumn("rank", functions.row_number()
						.over(Window.orderBy("avgprice")));
				
				//choosing best with low price and high popularity
				Dataset<Row> finalHotel = hotelCountRank.as("a").join(hotelPriceRank.as("b"),hotelCountRank.col("_c17").equalTo(hotelPriceRank.col("_c17")));
				String expr= "a.rank+b.rank";
				
				Dataset<Row> hotelRank=finalHotel.select(finalHotel.col("a._c17"),functions.expr(expr).as("rank")).
						withColumn("frank", functions.row_number().over(Window.orderBy("rank")));
				//hotelRank.show();
				hotelData.unpersist();
				
				
				//time preffered
				Dataset<Row> rawmonth = input.select(input.col("_c9"));
				
				Dataset<String> months=rawmonth.map((MapFunction<Row,String>) r ->r.toString().substring(6, 8),Encoders.STRING());
				
				Dataset<Row> monthf = months.groupBy("value").count().sort(functions.desc("count"));
				//monthf.show();
				
				//city and time preffered
				Dataset<Row> rawcitymonth = input.select(input.col("_c2"),input.col("_c9")).filter(input.col("_c2").isNotNull());
				Dataset<Row> citymonth = rawcitymonth.withColumn("_c9", functions.month(input.col("_c9")));
				Dataset<Row> citymonthrank = citymonth.groupBy(functions.col("_c2"),functions.col("_c9")).count();						
				//sorted data
				//citymonthrank.sort(functions.desc("count")).show();
				
				//duration of time
				Dataset<Row> rawduration = input.select(input.col("_c2"),input.col("_c9"),input.col("_c10"))
										.filter(input.col("_c2").isNotNull());
				
				Dataset<Row> duration= rawduration.withColumn("duration", functions.datediff(rawduration.col("_c10"),rawduration.col("_c9") ));
				
				Dataset<Row> cityduration=duration.groupBy(duration.col("_c2")).agg(functions.avg("duration").as("avgdur"));
				//cityduration.sort(functions.desc("avgdur")).show();
				
				//combo packs
				//flight
				Dataset<Row> rawflightdata = input.select(input.col("_c0"),input.col("_c11"),input.col("_c14"))
								.filter(input.col("_c0").isNotNull().or(input.col("_c11").notEqual(0.0)));
				rawflightdata.persist();
				Dataset<Row> flightdatatemp = rawflightdata.groupBy(functions.col("_c0").as("_c00")).agg(functions.min("_c11"));
				Dataset<Row> flightdata=flightdatatemp.filter(functions.col("min(_c11)").notEqual(0.0));
				flightdata.persist();
				Dataset<Row> flight=flightdata.as("a").join(rawflightdata.as("b"),flightdata.col("_c00").equalTo(rawflightdata.col("_c0"))
						.and(flightdata.col("min(_c11)").equalTo(rawflightdata.col("_c11"))));
				flight.persist();
				rawflightdata.unpersist();
				flightdata.unpersist();
				
				//car
				Dataset<Row> rawcardata = input.select(input.col("_c0"),input.col("_c12"),input.col("_c16"))
						.filter(input.col("_c0").isNotNull().or(input.col("_c12").notEqual(0.0)));
				rawcardata.persist();
				Dataset<Row> cardatatemp = rawcardata.groupBy(functions.col("_c0").as("_c00")).agg(functions.min("_c12"));
				Dataset<Row> cardata=cardatatemp.filter(functions.col("min(_c12)").notEqual(0.0));
				cardata.persist();
				Dataset<Row> car=cardata.as("a").join(rawcardata.as("b"),cardata.col("_c00").equalTo(rawcardata.col("_c0"))
						.and(cardata.col("min(_c12)").equalTo(rawcardata.col("_c12"))));
				car.persist();
				rawcardata.unpersist();
				cardata.unpersist();
				
				//hotel
				Dataset<Row> rawhoteldata = input.select(input.col("_c0"),input.col("_c13"),input.col("_c17"))
						.filter(input.col("_c0").isNotNull().or(input.col("_c13").notEqual(0.0)));
				rawhoteldata.persist();
				Dataset<Row> hoteldatatemp = rawhoteldata.groupBy(functions.col("_c0").as("_c00")).agg(functions.min("_c13"));
				Dataset<Row> hoteldata=hoteldatatemp.filter(functions.col("min(_c13)").notEqual(0.0));
				hoteldata.persist();
				Dataset<Row> hotel=hoteldata.as("a").join(rawhoteldata.as("b"),hoteldata.col("_c00").equalTo(rawhoteldata.col("_c0"))
						.and(hoteldata.col("min(_c13)").equalTo(rawhoteldata.col("_c13"))));
				hotel.persist();
				rawhoteldata.unpersist();
				hoteldata.unpersist();
				//trio
				Dataset<Row> triopack =flight.as("f").join(car.as("c"),"_c00").join(hotel.as("h"),"_c00");
				car.unpersist();
				hotel.unpersist();
				flight.unpersist();
				triopack.select(functions.col("f._c00"),functions.col("f._c11"),functions.col("f._c14"),
						functions.col("c._c12"),functions.col("c._c16"),functions.col("h._c13"),functions.col("h._c17")).show();
				
				
	}
	
}
