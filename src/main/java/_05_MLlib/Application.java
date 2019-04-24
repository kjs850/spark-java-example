package _05_MLlib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = new SparkSession.Builder()
                .appName("Example")
                .master("local")
                .getOrCreate();

        Dataset<Row> treatmentDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("src/main/resources/_05/cryotherapy.csv");

        Dataset<Row> lblFeatureDf = treatmentDf.withColumnRenamed("Result_of_Treatment", "label")
                .select("label", "sex","age","time","number_of_warts","type","area");

        lblFeatureDf = lblFeatureDf.na().drop(); // drop the rows containing any null or NaN values.
        lblFeatureDf.show();

        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("sex").setOutputCol("sexIndex");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String [] {"sexIndex", "age", "time", "number_of_warts", "type", "area"})
                .setOutputCol("features");


        Dataset<Row> [] splitData = lblFeatureDf.randomSplit(new double[] {.7, .3});
        Dataset<Row> trainingDf = splitData[0];
        Dataset<Row> testingDf = splitData[1];

        LogisticRegression logReg = new LogisticRegression();

        Pipeline pl = new Pipeline();
        pl.setStages(new PipelineStage[] {genderIndexer, assembler, logReg});

        PipelineModel model = pl.fit(trainingDf);
        Dataset<Row> results = model.transform(testingDf);

        results.show();

    }
}
