package org.sample.log;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * Based on the time difference between two consecutive request from same IP+user agent client type and suggested inactivity duration from the analysis step,
 * this step will mark the session boundary and then mark all the records with appropriate session id.
 */
public class SessionizeData {

    private SparkSession spark = null;

    public SessionizeData(SparkSession _spark){
        spark=_spark;
    }

    /**
     * Calculate lag between two consecutive request and if lag is more than 15 (passed as param) minutes mark as session boundary.
     * Use an 1/0 indicator for session boundary,so any event having a time difference greater than supplied value (e.g. 15 mins)
     * will be marked as '1' to be identified as a session boundary.
     * Another window function can sum up the indicator with unbounded preecding to group request in session
     */
    public Dataset sessionalize(Dataset baseset, long inactivityIntervalMill, String outPath) {


        WindowSpec wsSess = Window.partitionBy(col("ip"),col("clientyp")).orderBy(col("tsmil").asc()).rowsBetween(Window.unboundedPreceding(),0);

        Dataset finalset = baseset
                .withColumn("sessboundind",when(col("tsdif").gt(inactivityIntervalMill),1).otherwise(0))
                .withColumn("sessid",sum(col("sessboundind")).over(wsSess));

        finalset.show();

        if(outPath!=null)
            finalset.write().mode(SaveMode.Overwrite).json(outPath+"_SessionizeData");

        return finalset;
    }
}