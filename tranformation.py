#transformation.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import when,substring,concat_ws,upper,col,lower,trim,date_format,regexp_replace
import glob

# ขั้นตอนการ clean data สามารถดูได้ใน cleasing_data.ipynb
soure_data = "gs://retail-data-forcleansing/retail.csv"
destination_data = 'gs://asia-east2-dataengineer-641-b2b745b6-bucket/data/retail/retail_updated.csv'

def input_data(df):
    spark = SparkSession.builder.appName("My PySpark code").getOrCreate()
    df = spark.read.options(header='true', inferSchema='true').csv(df)
    return df
    
def output_data(df):
    retail_data.coalesce(1).write.format("csv").option("header", "true").mode('overwrite').save(df)
    

def ingest_transfrom_data(df):
    retail_data = create_timestamp_from_invoicedate(df)
    retail_data = update_invoiceNO(retail_data)
    retail_data = update_quantity(retail_data)
    retail_data = update_unitprice(retail_data)
    retail_data = update_stockprice(retail_data)
    retail_data = update_customerID(retail_data) 
    retail_data = update_column(retail_data)
    retail_data = update_description(retail_data)
    return retail_data

def create_timestamp_from_invoicedate(df):
    retail_data_update = df.withColumn('split', f.split('InvoiceDate', ' '))
    retail_data_update = retail_data_update.withColumn('datetime', f.col('split').getItem(0))
    retail_data_update = retail_data_update.withColumn("datetime",
                        f.to_date(retail_data_update.datetime, 'M/d/yyyy'))
    retail_data_update = retail_data_update.withColumn('time', f.col('split').getItem(1))
    retail_data_update = retail_data_update.withColumn('time', date_format('time', 'HH:mm:ss'))
    retail_data_update = retail_data_update.drop("InvoiceDate","split")
    retail_data_update = retail_data_update.withColumn('timestamp', f.concat('datetime', f.lit(' '), 'time'))
    retail_data_update = retail_data_update.drop("time").drop("datetime")
    retail_data_update = retail_data_update.withColumn("timestamp",
                        f.to_timestamp("timestamp", 'yyyy-MM-dd HH:mm:ss')
                        )
    # retail_data_update = retail_data_update.withColumn('timestamp', trim(retail_data_update['timestamp']))
    
    return retail_data_update

def update_invoiceNO(df):
    retail_data_update = df.withColumn("InvoiceNoUpdate",when(df['InvoiceNo'].rlike("^C[0-9]{6}$"), f.regexp_replace(df['InvoiceNo'], "^C([0-9]{6})$", "$1")).otherwise(df['InvoiceNo']))
    retail_data_update = retail_data_update.drop("InvoiceNo").withColumnRenamed('InvoiceNoUpdate', 'InvoiceNo')
    retail_data_update = retail_data_update.withColumn('InvoiceNO', trim(retail_data_update['InvoiceNo']))
    return retail_data_update

def update_quantity(df):
    retail_data_update = df.withColumn("QuantityUpdate", when(df['Quantity'] < 0, df['Quantity']*-1).otherwise(df['Quantity']))
    retail_data_update = retail_data_update.drop("Quantity").withColumnRenamed('QuantityUpdate', 'Quantity')
    
    return retail_data_update

def update_unitprice(df):
    retail_data_update = df.withColumn("UnitPriceUpdate", when(df['UnitPrice'] < 0, df['UnitPrice']*-1).otherwise(df['UnitPrice']))
    retail_data_update = retail_data_update.drop("UnitPrice").withColumnRenamed('UnitPriceUpdate', 'UnitPrice')
    
    return retail_data_update

def update_stockprice(df):
    retail_data_update = df.withColumn('StockCodeUpdate',df['StockCode'])
    retail_data_update = retail_data_update.withColumn('StockCodeUpdate', when(retail_data_update['StockCodeUpdate'].rlike('[a-zA-Z][a-zA-Z]$'), substring('StockCodeUpdate', 1, 6)).otherwise(retail_data_update['StockCodeUpdate']))
    retail_data_update = retail_data_update.withColumn("StockCodeUpdate",when(col("StockCodeUpdate").rlike("[0-9]{5}[a-z]$"),concat_ws("", substring(col("StockCodeUpdate"), 1, 5), upper(substring(col("StockCodeUpdate"), -1, 1)))).otherwise(col("StockCodeUpdate")))
    retail_data_update = retail_data_update.withColumn('StockCodeUpdate',when(~retail_data_update["StockCodeUpdate"].rlike("[0-9][0-9][0-9][0-9][0-9][A-Z]$") & ~retail_data_update["StockCodeUpdate"].rlike("[0-9]{5}$"),'00000').otherwise(retail_data_update['StockCodeUpdate']))
    retail_data_update = retail_data_update.drop("StockCode").withColumnRenamed('StockCodeUpdate', 'StockCode')
    retail_data_update = retail_data_update.withColumn('StockCode', trim(retail_data_update['StockCode']))
    return retail_data_update

def update_customerID(df):
    retail_data_update = df.withColumn("CustomerID", col("CustomerID").cast("integer"))
    retail_data_update = retail_data_update.withColumn("CustomerID", col("CustomerID").cast("string"))
    retail_data_update = retail_data_update.withColumn("CustomerID", when(retail_data_update['CustomerID'].isNull(),'00000').otherwise(retail_data_update['CustomerID']))
    retail_data_update = retail_data_update.withColumn("CustomerID", when(retail_data_update['CustomerID'] == '0','00000').otherwise(retail_data_update['CustomerID']))
    retail_data_update = retail_data_update.withColumn("CustomerID", col("CustomerID").cast("string"))
    retail_data_update = retail_data_update.withColumn('CustomerID', trim(retail_data_update['CustomerID']))
    return retail_data_update
    
def update_column(df):
    retail_data_update = df.toDF(*[col.lower() for col in df.columns])
    return retail_data_update



def update_description(df):
    retail_data_update = df.withColumn("description", when(df['description'].isNull(),'No Description').otherwise(df['description']))
    retail_data_update = retail_data_update.withColumn("description", when(retail_data_update['description'] == '?','No Description').otherwise(retail_data_update['description']))
    
    retail_data_update = retail_data_update.withColumn('description', regexp_replace('description', "'", ''))
    retail_data_update = retail_data_update.withColumn('description', regexp_replace('description', '\\"', ''))
    # retail_data_update = retail_data_update.withColumn("description", regexp_replace("description", "\s+", "_"))
    return retail_data_update

    
if __name__ == "__main__":
    df = input_data(soure_data)
    retail_data = ingest_transfrom_data(df)
    output_data(destination_data)
   
    



# retail_data.coalesce(1).write.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").mode('overwrite').save("gs://us-central1-data-pipeline-c-a54e9a00-bucket/data/retail/retail_updated.csv")