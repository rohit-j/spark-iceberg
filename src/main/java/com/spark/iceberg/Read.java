package com.spark.iceberg;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriterV2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Read {
    public static SparkSession getSparkSession(Properties loadConfig) {
        // Spark Session Properties
        SparkSession.Builder sparkBuild = SparkSession.builder().appName("Spark Iceberg Read.");
        loadConfig.forEach((key,value) -> {
            String loadKey = (String) key;
            if(loadKey.startsWith("spark.")) {
                System.out.println(loadKey + " = " + (String) value);
                sparkBuild.config(loadKey, (String) value);
            }
        });
        SparkSession spark = sparkBuild.getOrCreate();

        String logOutput = loadConfig.getProperty("logOutput");
        if(logOutput!=null && (logOutput.equals("INFO") || logOutput.equals("DEBUG") || logOutput.equals("ERROR")))  spark.sparkContext().setLogLevel(logOutput);
        else spark.sparkContext().setLogLevel("ERROR");
        
        return spark;
    }
  
    public static void main (String args[]) throws Exception{
        if(args.length != 1) {
            System.out.println("Please provide Spark Iceberg Read Config properties filepath as an argument.\n");
            System.exit(0);
        }

        Properties sparkDataloadProperties = new Properties();
        try (FileInputStream input = new FileInputStream(args[0])) {
            sparkDataloadProperties.load(input);
        }catch (IOException e) { 
            e.printStackTrace(); 
            throw e;
        }

        String sqlQuery = sparkDataloadProperties.getProperty("sqlQuery");
        if(sqlQuery==null || sqlQuery.isBlank()) {
            throw new Exception("Please provide 'sqlQuery' option in the read config properties.");
        }
        if (sqlQuery != null && sqlQuery.startsWith("\"") && sqlQuery.endsWith("\"")) {
            sqlQuery = sqlQuery.substring(1, sqlQuery.length() - 1);
        }

        String fetchRows = sparkDataloadProperties.getProperty("fetchRows");
        if(fetchRows==null || fetchRows.isBlank()) {
            throw new Exception("Please provide 'fetchRows' option in the read config properties.");
        }
        
        String explainMode = sparkDataloadProperties.getProperty("explainMode");
        if(explainMode==null || explainMode.isBlank()) {
            throw new Exception("Please provide 'explainMode' option in the read config properties.");
        }

        SparkSession spark = getSparkSession(sparkDataloadProperties);
        System.out.println("Executing Spark SQL Query: "+sqlQuery);  
        Dataset<Row> df = spark.sql(sqlQuery);
        System.out.println("Printing rows");  
        df.show(Integer.parseInt(fetchRows));
        System.out.println("Printing dataframe explain with specified mode.");  
        if(explainMode.equalsIgnoreCase("true") || explainMode.equalsIgnoreCase("false")){
            df.explain(Boolean.parseBoolean(explainMode.toLowerCase()));
        } else if(explainMode.equals("simple") || explainMode.equals("extended")
               || explainMode.equals("codegen") || explainMode.equals("cost") || explainMode.equals("formatted")){
            // 'simple', 'extended', 'codegen', 'cost', 'formatted'
            df.explain(explainMode);
        } else {
            df.explain(false);
        }
        spark.stop();
    }
}