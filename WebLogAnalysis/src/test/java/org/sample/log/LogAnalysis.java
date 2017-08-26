package org.sample.log;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Main program, coordinating the data prep,inactivity interval analysis, sessionize process and showing required results.
 */
public class LogAnalysis {

    public static  void main(String args[]){

        //TODO: check for # of params and validate, include more logic to skip intermediate data persistence

        String inPath = args[0];
        String outPath = args[1];

        SparkSession spark = SparkSession.builder().master("local").appName("WebLogAnalysis").config("spark.driver.host", "localhost").getOrCreate();

        DataPrep dataPrep = new DataPrep(spark);
        Dataset dataPrepSet= dataPrep.doPrep(inPath,outPath);

        IntervalAnalysis intervalAnalysis = new IntervalAnalysis(spark);
        long interVal = intervalAnalysis.suggestInterval(outPath);

        System.out.println("Suggested inactivity duration in seconds " + interVal);

        SessionizeData sessionizeData = new SessionizeData(spark);
        Dataset sessionSet = sessionizeData.sessionalize(dataPrepSet,interVal,null);
        ShowAnalysis showAnalysis = new ShowAnalysis(spark);
        showAnalysis.showAnalysis(sessionSet);

    }
}