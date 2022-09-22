#import required libraries

from pyspark.sql import SparkSession
import pyspark.sql.functions as pyf
from pyspark.sql.types import *



#create spark session
spark = SparkSession.builder.appName("Project_kafka").getOrCreate()
spark.sparkContext.setLogLevel('Error')


#Define UDF's
#UDF to determine total_sales
def total_sales(type,unit_price,quantity):
    if (type == 'RETURN'):
        return (-1*(unit_price*quantity))
    else:
                return (unit_price*quantity)

# UDF to determine Is_Return in case of Return.
def Is_Return(type):
    if type == "RETURN":
        return(1)
    else:
        return(0)

# UDF to determine Is_Order in case of Order.
def Is_Order(type):
    if type == "ORDER":
        return(1)
    else:
        return(0)


#read from the kafka server
lines=spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092") \
    .option("subscribe","real-time-project") \
    .load()

lines.printSchema()

#define schema

Schema=ArrayType(StructType([
                StructField("items", ArrayType(StructType([
                                    StructField("SKU", StringType()),
                                    StructField("title", StringType()),
                                    StructField("unit_price", FloatType()),
                                    StructField("quantity", IntegerType())]))),
                StructField("type", StringType(),True),
                StructField("country", StringType(),True),
                StructField("invoice_no", StringType(),True),
                StructField("timestamp", TimestampType(),True),
]))


#Since the value is in binary, first we need to convert the binary value to String using selectExpr()
inp_df = lines.selectExpr("CAST(key AS STRING)","CAST(value AS STRING) as Input_Records").select(pyf.from_json("Input_Records",Schema).alias("Input_Records"))

#using explode function to split an array or map DataFrame columns to rows.

stg_df = inp_df.select(pyf.explode("Input_Records").alias("staging")).select("staging.timestamp","staging.country","staging.invoice_no","staging.type",(pyf.explode("staging.items").alias("items")))


#renaming cols
final_df=stg_df.withColumnRenamed("staging.timestamp", "timestamp").withColumnRenamed("staging.country", "country").withColumnRenamed("staging.invoice_no", "invoice_no").withColumnRenamed("staging.type" , "type")

final_df.printSchema()

#here all column values in arrays  are now split into single cols
df1=final_df.select(pyf.col('timestamp'),pyf.col('country'),pyf.col('invoice_no'),pyf.col('type') \
        ,pyf.col('items')['SKU'].alias('SKU') \
        ,pyf.col('items')['title'].alias('title') \
        ,pyf.col('items')['unit_price'].alias('unit_price') \
        ,pyf.col('items')['quantity'].alias('quantity'))



# Declare user def function for total sales

total_sales=pyf.udf(total_sales,FloatType())

# Adding total_sales to data frame

df1=df1.withColumn("total_cost", total_sales(df1.type,df1.unit_price,df1.quantity))

# Declare user def function for Is Return
Is_Return = pyf.udf(Is_Return, IntegerType())

# Adding Is Return flag to existing DF
df1 = df1.withColumn("Is_Return", Is_Return(df1.type))

# Declare user def function Is Order
Is_Order = pyf.udf(Is_Order, IntegerType())

# Adding Is Order flag to existing DF
df1 = df1.withColumn("Is_Order", Is_Order(df1.type))

# write input data ater adding all derived columns to console for each order for window of 1 minute.
df2 = df1.withWatermark("timestamp","1 second") \
                 .groupby(pyf.window("timestamp","1 minute"),"invoice_no","country","Is_Order","Is_Return") \
         .agg(pyf.sum("total_cost").alias("total_cost"),pyf.sum("quantity").alias("total_items"))


#calculating time based KPI's
df3=df1.select("invoice_no","timestamp","total_cost","quantity","Is_Order","Is_Return")
time_based_kpi = df3.withWatermark("timestamp", "10 minutes") \
                        .groupby(pyf.window("timestamp","1 minute")) \
                        .agg(pyf.sum("total_cost").alias("Total_sales_vol"), \
                        pyf.approx_count_distinct("invoice_no").alias("OPM"), \
                        pyf.sum("Is_Order").alias("total_Order"), \
                        pyf.sum("Is_Return").alias("total_return"), \
                        pyf.sum("quantity").alias("total_items"))

#KPI: rate of return
time_based_kpi = time_based_kpi.withColumn("rate_of_return",time_based_kpi.total_return/(time_based_kpi.total_Order+time_based_kpi.total_return))

#KPI: average transaction size
time_based_kpi = time_based_kpi.withColumn("Avg_trans_size",time_based_kpi.Total_sales_vol/(time_based_kpi.total_Order+time_based_kpi.total_return))
time_based_kpi = time_based_kpi.select("window","OPM","Total_sales_vol","Avg_trans_size","rate_of_return")


#calculating Country and Time Based KPI

df4= df1.select("country","invoice_no","timestamp","total_cost","quantity","Is_Order","Is_Return")
Final = df4.withWatermark("timestamp", "10 minutes") \
                        .groupby(pyf.window("timestamp","1 minute"),"country") \
                        .agg(pyf.sum("total_cost").alias("Total_sales_vol"), \
                        pyf.approx_count_distinct("invoice_no").alias("OPM"), \
                        pyf.sum("invoice_no").alias("sum_invoice"), \
                        pyf.sum("Is_Order").alias("total_Order"), \
                        pyf.sum("Is_Return").alias("total_return"), \
                        pyf.sum("quantity").alias("total_items"))

#KPI for rate of return
Final = Final.withColumn("rate_of_return",Final.total_return/(Final.total_Order+Final.total_return))
Final_country_time = Final.select("window","country","OPM","Total_sales_vol","rate_of_return")

# printing output on console for the first dattaframe craeted after adding udf's . comment this out for running only KPI code.
query_1 = df2 \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "False")  \
        .start()


#Printing Time based KPI to HDFS as Json file
query_2 = time_based_kpi.writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","/user/root/Retail_data_project/Time_based_KPI") \
    .option("checkpointLocation", "/user/root/Retail_data_project/Time_based_KPI/timebased_json") \
    .trigger(processingTime="1 minute") \
    .start()

#Printing Time and Country KPI to HDFS as Json file

query_3 = Final_country_time.writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","/user/root/Retail_data_project/Time_country_KPI") \
    .option("checkpointLocation", "/user/root/Retail_data_project/Time_country_KPI/time_country_json") \
    .trigger(processingTime="1 minute") \
    .start()
query_1.awaitTermination()
query_1.awaitTermination()
query_3.awaitTermination()


