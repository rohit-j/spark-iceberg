package com.spark.iceberg;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriterV2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Load {
	
	public static SparkSession getSparkSession(Properties loadConfig) {
		// Spark Session Properties
		SparkSession.Builder sparkBuild = SparkSession.builder().appName("Spark Data Load.");
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
			System.out.println("Please provide Spark Data Load Config properties filepath as an argument.\n");
			System.exit(0);
		}

		Properties sparkDataloadProperties = new Properties();
        try (FileInputStream input = new FileInputStream(args[0])) {
        	sparkDataloadProperties.load(input);
        }catch (IOException e) { 
        	e.printStackTrace(); 
        	throw e;
        }
        
        // Source Format
		String loadFormat = sparkDataloadProperties.getProperty("format");
		if(loadFormat==null || loadFormat.isBlank()) throw new Exception("Please provide 'loadFormat' option in the load config properties.");
		if(loadFormat!= null && !(loadFormat.equals("jdbc") || loadFormat.equals("parquet") || loadFormat.equals("iceberg") || loadFormat.equals("csv") || loadFormat.equals("sql"))) {
			throw new Exception("Unknown format provided: "+ loadFormat+ ". Supported formats: jdbc|parquet|iceberg|csv|sql");
		}
		
		// Source Path for parquet or csv
		String sourcePath = sparkDataloadProperties.getProperty("sourcePath");
		if(  (loadFormat.equals("parquet") || loadFormat.equals("csv")) && (sourcePath==null || sourcePath.isBlank()) ) {
			throw new Exception("Please provide 'sourcePath' option in the load config properties for parquet or csv formats.");
		}
			
		// Additional read and write options
		Map<String, String> readOptions = new HashMap<String, String>();
		Map<String, String> writeOptions = new HashMap<String, String>();
		sparkDataloadProperties.forEach((key,value) -> {
			String loadKey = (String) key;
			if(loadKey.startsWith("readOption.")) {
				readOptions.put(loadKey.replace("readOption.",""), (String)value );
			} else if(loadKey.startsWith("writeOption.")) {
				writeOptions.put(loadKey.replace("writeOption.",""), (String)value );
			}
		});
		
		String namespace = sparkDataloadProperties.getProperty("namespace");
		if(namespace==null || namespace.isBlank()) throw new Exception("Please provide 'namespace' option in the load config properties.");
		
		// List of tables (for jdbc, parquet, csv)
		String tableList = sparkDataloadProperties.getProperty("tableList");
		if( (loadFormat.equals("jdbc") || loadFormat.equals("parquet") || loadFormat.equals("iceberg") || loadFormat.equals("csv")) && (tableList==null || tableList.isBlank()) )
			throw new Exception("Please provide 'tableList' option in the load config properties, having comma seperated list of tables.");
		
		// OR
		
		// Target name for sql load
		String sqlQuery = sparkDataloadProperties.getProperty("sqlQuery");
		if (sqlQuery != null && sqlQuery.startsWith("\"") && sqlQuery.endsWith("\"")) {
            sqlQuery = sqlQuery.substring(1, sqlQuery.length() - 1);
        }
		String targetIcebergName = sparkDataloadProperties.getProperty("targetIcebergName");
		if( loadFormat.equals("sql") && ( (sqlQuery==null || sqlQuery.isBlank()) || (targetIcebergName==null || targetIcebergName.isBlank()) ) ) {
			throw new Exception("Please provide 'sqlQuery' and 'targetIcebergName' option in the load config properties for sql load format.");
		}
		
		// All properties fetched and validated. Spark read and write.
		SparkSession spark = getSparkSession(sparkDataloadProperties);
		
		if(loadFormat.equals("sql")) {
			System.out.println("Executing Spark SQL Query: "+sqlQuery);  
			System.out.println("Spark Read Options: "+readOptions.toString());
			Dataset<Row> df = spark.sql(sqlQuery);
			System.out.println("Spark Write Iceberg: "+"dev."+namespace+"."+targetIcebergName);  
			System.out.println("Spark Write Options: "+writeOptions.toString());
			DataFrameWriterV2<Row> writeBuilder = df.writeTo("dev."+namespace+"."+targetIcebergName);//.create();
	   		for (Map.Entry<String, String> entry : writeOptions.entrySet()) {
	   		    writeBuilder.tableProperty(entry.getKey(), entry.getValue());
	   		}
	   		writeBuilder.create();
		} else {
			// Read Source Table List
			for(String tableName: tableList.split(",")) {
				tableName = tableName.trim();
			    if (!tableName.isEmpty()) {
			        Dataset<Row> df = null;
			        System.out.println("Spark Read: "+tableName+". Format: "+loadFormat); 
			   		System.out.println("Spark Read Options: "+readOptions.toString());
			   		if(loadFormat.equals("jdbc")) {
			   			df = spark.read().format("jdbc").options(readOptions).option("dbtable", tableName.toUpperCase()).load();
			   		} else if(loadFormat.equals("parquet")) {
			   			df = spark.read().options(readOptions).parquet(sourcePath + "/" + tableName);  
			   			tableName = tableName.replace(".","_");
			   		} else if (loadFormat.equals("csv")){
			   			df = spark.read().options(readOptions).csv(sourcePath + "/" + tableName);
				        tableName = tableName.replace(".","_");
			   		} else if (loadFormat.equals("iceberg")){
			   			df = spark.read().format("iceberg").options(readOptions).load(tableName);
			   			String[] parts = tableName.split("\\.");
				        tableName = parts[parts.length - 1];
			   		}
			   		System.out.println("Spark Write Iceberg: "+"dev."+namespace+"."+tableName.toUpperCase());  
			   		System.out.println("Spark Write Options: "+writeOptions.toString());
			   		DataFrameWriterV2<Row> writeBuilder = df.writeTo("dev."+namespace+"."+tableName.toUpperCase());//.create();
			   		for (Map.Entry<String, String> entry : writeOptions.entrySet()) {
			   		    System.out.println("Setting iceberg table property: "+entry.getKey()+" -> "+entry.getValue());
			   		    writeBuilder.tableProperty(entry.getKey(), entry.getValue());
			   		}
			   		writeBuilder.create();
			    }
			}
		}
		String keepAlive = sparkDataloadProperties.getProperty("keepAlive");
		if (keepAlive != null && keepAlive.equals("true")){
			// Add a shutdown hook to stop the Spark session on exit
	        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	            System.out.println("Stopping Spark session.");
	            spark.stop();
	        }));
	        
	        // Keep main running
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await();
		} else {
			spark.stop();
		}
	}
}