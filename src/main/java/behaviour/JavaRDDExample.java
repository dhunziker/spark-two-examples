package behaviour;

import org.apache.http.util.Asserts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Dennis Hunziker on 20/11/16.
 */
public class JavaRDDExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("My App").setMaster("local[1]");
        JavaSparkContext spark = new JavaSparkContext(conf);

        List<String> data = new LinkedList();
        data.add("Foo Bar");
        data.add("Hello World");

        JavaRDD<String> rdd = spark.parallelize(data);
        JavaPairRDD<Integer, String> rdd1 = rdd.keyBy(row -> 1);

        // Returns Map instead of Object now
        Asserts.check(rdd1.countByKey().get(1) == 2, "Count must be 2");

        // https://issues.apache.org/jira/browse/SPARK-3369
        FlatMapFunction<Iterator<String>, Integer> mapToLength = (Iterator<String> in) -> new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return in.hasNext();
            }

            @Override
            public Integer next() {
                return in.next().length();
            }
        };

        JavaRDD<Integer> rdd2 = rdd.mapPartitions(mapToLength);
        rdd2.collect().stream().forEach(System.out::println);
    }

}
