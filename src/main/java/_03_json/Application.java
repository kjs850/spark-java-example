package _03_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Json Line to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df1 =  spark.read().format("json")
                .load("src/main/resources/_03/simple.json");

        df1.show(5, 150);
        df1 = df1.filter(df1.col("owns").isNotNull());
        df1.show(5, 150);

        df1.printSchema();

        Dataset<Row> df2 =  spark.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/_03/multiline.json");

        df2.show(5, 150);
        df2.printSchema();
    }
}
