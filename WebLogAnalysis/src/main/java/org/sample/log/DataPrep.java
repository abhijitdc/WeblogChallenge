package org.sample.log;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

/**
 * This class loads,transforms and extracts the required colums from raw data.
 * IP extracted from client-port
 * URL path extracted from request
 * Client machine type extracted from user agent
 * timestamp is converted into seconds
 * Records without client machine type gets filtered.
 * Filter records where for a group of IP and client machine type there is only one record
 * time difference between two consecutive request for same IP and client machine type is calculated
 */
public class DataPrep {

    private SparkSession spark = null;

    public  DataPrep(SparkSession _spark){
        spark=_spark;
    }

    private  Dataset loadData(String inPath) {
        return loadData(inPath,null);
    }

    private  Dataset loadData(String inPath,String outPath) {

        //TODO: Convert the schema string into an ENUM with appropriate data type at a central place.

        // The schema is encoded in a string
        String schemaString = "elb,clientport,backend-port,request_processing_time,backend_processing_time,response_processing_time,elb_status_code,backend_status_code," +
                "received_bytes,sent_bytes,request,useragent,ssl_cipher,ssl_protocol";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
        for (String fieldName : schemaString.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> lograw = spark.read().option("sep", " ").schema(schema).csv(inPath);

        if(outPath!=null)
            lograw.write().mode(SaveMode.Overwrite).json(outPath+"_RAWDATA");

        return lograw;

    }

    private Dataset dataprep(Dataset lograw) {
        return dataprep(lograw,null);
    }

    /**
     * create baseset with following additional col
     * a timestamp in seconds
     * extract URL:PATH from request
     * extract IP from client-port
     * Using user agent client machine type with IP to identify unique users
     */

    private Dataset dataprep(Dataset lograw, String outPath) {

        spark.udf().register("getUrlPath",(String url)->new URL(url).getPath(),DataTypes.StringType);


        Dataset baseset = lograw.select("timestamp", "clientport", "request", "useragent").withColumn("tsmil",unix_timestamp(col("timestamp")))
                .withColumn("ip",split(col("clientport"),":").getItem(0)).withColumn("urlpath",callUDF("getUrlPath",split(col("request")," ").getItem(1)))
                .withColumn("clientyp",regexp_extract(col("useragent"),"^(.*?)\\s\\((.*?)\\)\\s(.*?)$",2));

        //TODO: Filter URL path for web resources like icon and images.

        //Filter records with no client machine type
        baseset = baseset.filter(length(trim(col("clientyp"))).gt(0));

        //Filter group with just one request
        WindowSpec wsFilterSingeReq = Window.partitionBy(col("ip"),col("clientyp")).orderBy(col("tsmil").asc()).rowsBetween(Window.unboundedPreceding(),Window.unboundedFollowing());
        baseset = baseset.withColumn("isSingle",count(col("ip")).over(wsFilterSingeReq)).filter(col("isSingle").gt(1));

        WindowSpec ws = Window.partitionBy(col("ip"),col("clientyp")).orderBy(col("tsmil").asc()).rowsBetween(-1,0);
        Dataset tsdifset = baseset.withColumn("lagts",first(col("tsmil")).over(ws))
                .withColumn("tsdif",col("tsmil").minus(col("lagts")));

        //TODO: Persist BaseSet

        if(outPath!=null)
            tsdifset.write().mode(SaveMode.Overwrite).json(outPath+"_PREPDATA");

        return tsdifset;
    }

    public Dataset doPrep(String inPath, String outPath) {

        Dataset rawData = loadData(inPath,outPath);
        return dataprep(rawData,outPath);

    }

    public void doPrep(String inPath) {

        Dataset rawData = loadData(inPath);
        dataprep(rawData);

    }
}