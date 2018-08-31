import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
public class WorDCountRDD {
public static void main(String[] args) {
	SparkSession spark = SparkSession.builder().appName("wordcount").master("local").getOrCreate();
	
	//spark.read().textfile return dataset row
	JavaRDD<String> file = spark.read().textFile("/Users/rushikesh/Desktop/abc.txt").toJavaRDD();
	JavaRDD<String> input = file.map(line->(line.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")).toLowerCase());
	// flatMaptoPair
	JavaPairRDD<String,Integer> words = input.flatMapToPair(line -> {
		ArrayList<Tuple2<String,Integer>> tp = new ArrayList<>();
		for (String word : line.split(" ")) {
			if(!word.isEmpty())
			tp.add(new Tuple2<>(word,1));
		}
		return tp.iterator();
	});
	
	//reduce
	JavaPairRDD<String, Integer> wordCount = words.reduceByKey((x, y) -> (x+y));
	//sortedWordCount
	JavaPairRDD<String, Integer> sortedWordCount = wordCount.mapToPair(s->s.swap()).sortByKey(false).mapToPair(s->new Tuple2<>(s._2,s._1));
	
	sortedWordCount.saveAsTextFile("/Users/rushikesh/Documents/Hadoopmain/sparkjars/outputsortedwc5");
	
	
}
}
