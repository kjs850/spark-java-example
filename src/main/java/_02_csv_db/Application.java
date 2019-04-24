package _02_csv_db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

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
                .load("");

        df.show(3);

        // transformation
        df = df.withColumn("full_name",
                concat(df.col("last_name"), lit(", "), df.col("first_name")))
                .filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc());

        df.show(3);

        // Write to destination
		String dbConnectionUrl = "jdbc:mysql://127.0.0.1:3306/testdb";
		Properties prop = new Properties();
	    prop.setProperty("driver", "com.mysql.jdbc.Driver");
	    prop.setProperty("user", "testuser");
	    prop.setProperty("password", "password");

        df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(dbConnectionUrl, "table1", prop);

    }
}
