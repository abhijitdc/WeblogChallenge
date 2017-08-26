package org.sample.log;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

/**
 * This class would attempt to find the most suitable inactivity duration to establish a session boundary.
 * Assuming user behavior is completely random,2 standard deviation value for the inactivity interval
 * between two consecutive request from same IP and client machine (based on user agent) will give us the threshold with
 * 95% probability (empirical rule) that all user will always have a interaction interval below that threshold.
 * This value then can be used as the session boundary. To reduce the effect of the outliers we will perform a min-max normalization before calulating the 2 SD value
 */
public class IntervalAnalysis {

    private SparkSession spark = null;

    public IntervalAnalysis(SparkSession _spark){
        spark=_spark;
    }

    public long suggestInterval(String inPath){

        //Load the data saved from the data prep step

        Dataset baseSet = spark.read().json(inPath+"_PREPDATA").select(col("ip"),col("clientyp"),col("tsdif"),col("tsmil"));

        //Perform min-max normalization for the interval value
        Dataset tempSet = baseSet.agg(coalesce(min("tsdif"),lit(0)).alias("tsmin"),coalesce(max("tsdif"),lit(0)).alias("tsmax"))
                .withColumn("tsdifrange",col("tsmax").minus(col("tsmin")));

        tempSet.show();

        Dataset zBaseSet = baseSet.crossJoin(tempSet)
                .withColumn("tsdifnorm",col("tsdif").minus(col("tsmin")).divide(col("tsdifrange")));



        List<Long> minVal = tempSet.select(col("tsmin").cast("long")).as(Encoders.LONG()).collectAsList();
        List<Long> rangeVal = tempSet.select(col("tsdifrange").cast("long")).as(Encoders.LONG()).collectAsList();

        //Find out the mean and SD
        Dataset metrics = zBaseSet.agg(avg(col("tsdifnorm")).alias("tsdifmean"),stddev_pop(col("tsdifnorm")).alias("tsdifsd"));


        metrics.show();


        List<Double> meanVal = metrics.select(col("tsdifmean")).as(Encoders.DOUBLE()).collectAsList();
        List<Double> sdVal = metrics.select(col("tsdifsd")).as(Encoders.DOUBLE()).collectAsList();


        //Calculate 2 SD value and reverse transform from min-max normalized value


        double interval = (meanVal.get(0)*rangeVal.get(0))+(2*sdVal.get(0)*rangeVal.get(0))+minVal.get(0);

        System.out.println("Suggested Interval " + interval + " Rounded " + Math.round(interval));

        return Math.round(interval);

    }
}