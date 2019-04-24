package _04_kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Application {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("StreamingKafkaConsumer")
                .master("local")
                .getOrCreate();

        Dataset<Row> messagesDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(value AS STRING)");

        Dataset<String> words = messagesDf
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();


        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }

    // 참고 카프카 로컬 구동 (for mac)

    // 설치
    // brew install kafka

    // 주키퍼 구동
    // zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

    // 카프카 구동
    // kafka-server-start /usr/local/etc/kafka/server.properties

    // 콘솔 프로듀서
    // kafka-console-producer --broker-list localhost:9092 --topic test

    // 콘솔 컨슈머
    // kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
}
