package _01_csv;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {

    public static void main(String args[]) throws InterruptedException {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("csv to db")
                .master("local")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/_01/name_and_comments.csv");
        df.show(3);

        // transformation
        df = df.withColumn("full_name",
                concat(df.col("last_name"), lit(", "), df.col("first_name")))
                .filter(df.col("comment").rlike("\\d+")) // 숫자가 들어간 코멘트
                .orderBy(df.col("last_name").asc());

        df.show(3);

    }
}
