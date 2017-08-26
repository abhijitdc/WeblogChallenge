package org.sample.log;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

/**
 * This class shows the required findings. Can be executed as standalone by providing the path of sessionize data.
 */
public class ShowAnalysis {
    private SparkSession spark = null;

    public ShowAnalysis(SparkSession _spark){
        spark=_spark;
    }

    public void showAnalysis(Dataset finalset) {

        System.out.println("Avg session length in seconds");
        /** Determine the average session time **/
        finalset.groupBy(col("ip"),col("clientyp"),col("sessid")).agg(max(col("tsmil")).alias("maxts"),min(col("tsmil")).alias("mints"))
                .withColumn("sesslength",col("maxts").minus(col("mints"))).select(mean(col("sesslength"))).show();

        /** Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.**/
        Dataset sessUrlSet = finalset.groupBy(col("ip"),col("clientyp"),col("sessid")).agg(countDistinct(col("urlpath")).alias("urlset"));

        System.out.println("Avg URL visits per session");
        sessUrlSet.select(mean(col("urlset"))).show();

        /** Find the most engaged users, ie the IPs with the longest session times **/
        Dataset sessWithDur = finalset.groupBy(col("ip"),col("clientyp"),col("sessid")).agg(max(col("tsmil")).alias("maxts"),min(col("tsmil")).alias("mints"))
                .withColumn("sesslength",col("maxts").minus(col("mints")));


        List<Long> maxSessionLength = sessWithDur.agg(max(col("sesslength"))).as(Encoders.LONG()).collectAsList();

        System.out.println("Max session length " + maxSessionLength.get(0));
        System.out.println("Most engaged user ");
        sessWithDur.filter(col("sesslength").equalTo(maxSessionLength.get(0))).show();


    }

    public static void main(String args[]){
        SparkSession spark = SparkSession.builder().master("local").appName("WebLogAnalysis").config("spark.driver.host", "localhost").getOrCreate();
        ShowAnalysis show = new ShowAnalysis(spark);
        Dataset sessionizeData = spark.read().json(args[0]+"_SessionizeData");
        show.showAnalysis(sessionizeData);
    }
}