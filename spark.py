import os
from sshtunnel import SSHTunnelForwarder
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
from pyspark.ml.regression import LinearRegression,LinearRegressionModel
from pyspark.sql.types import *
from pyspark.ml.regression import GBTRegressor

near = {12:[13,261,88],88:[12,261,87],87:[88,261,209],13:[231,261,12],261:[13,231,209,87,88,12],209:[87,261,231,45],
            231:[13,261,209,45,144,211,125],45:[209,231,144,148,232],
            125:[158,249,114,211,231],211:[125,114,144,231],144:[45,231,211,114,148],148:[45,144,79,232],232:[45,148,4],
            158:[246,68,249,125],249:[158,68,90,113,114,125],114:[125,211,144,79,249],113:[114,49,234,79],79:[113,114,107,224,4,148],4:[232,79,224],
            246:[50,48,68,158],68:[246,48,100,90,186,249,158],90:[68,186,234,249],186:[68,19,164,90,234],100:[68,48,230,164,186],164:[186,100,161,170,234],234:[90,186,164,107,113],107:[79,224,234,137,170],224:[4,70,107,130],137:[224,107,170,233],170:[137,107,164,161,162,233],
            50:[246,48,143],48:[68,246,50,142,163,230,100],230:[100,48,163,161],161:[164,230,163,162,170],162:[170,161,163,237,229,233],229:[233,162,141,140],233:[137,170,162,229],163:[43,237,162,161,230,48],
            143:[50,142,239],142:[48,143,239,43,163],43:[142,239,238,151,24,41,75,236,237,163],237:[162,162,43,236,141],141:[237,236,263,140,229],140:[229,141,262],
            239:[142,142,43,238],238:[239,151,43],236:[237,43,75,263],263:[141,236,75,262],262:[140,265,75],
            151:[24,238,43],24:[166,41,43,151],75:[236,263,262,43,41,74],
            166:[24,41,152],41:[43,24,166,42,74],74:[75,41,42],
            152:[42,166,116],42:[41,74,120,116,152],116:[152,42,120,244],
            244:[120,116,243],120:[244,243,42,116],243:[120,244,128,127],
            127:[128,120,243],128:[243,127]}

locs = {12:[1,1],88:[1,2],13:[2,1],261:[2,2],87:[2,3],231:[3,1],209:[3,2],45:[3,2],
        125:[4,1],211:[4,2],144:[4,3],148:[4,4],232:[4,5],158:[5,1],249:[5,2],114:[5,3],113:[5,3],79:[5,4],4:[5,5],
        246:[6,1],68:[6,2],90:[6,3],186:[6,3],100:[6,3],234:[6,4],164:[6,4],107:[6,5],170:[6,5],137:[6,6],224:[6,6],
        50:[7,1],48:[7,2],230:[7,3],163:[7,3],161:[7,4],162:[7,5],229:[7,6],233:[7,6],
        143:[8,1],142:[8,2],43:[8,3],237:[8,4],141:[8,5],140:[8,6],
        239:[9,1],238:[9,1],236:[9,3],263:[9,4],262:[9,5],
        151:[10,1],24:[10,1],75:[10,3],166:[11,1],41:[11,2],74:[11,3],
        152:[12,1],42:[12,2],116:[12,1],244:[13,1],243:[14,1],120:[13,2],128:[15,1],127:[15,2]
        }
def func(pick,drop):
    if drop in near[pick]:
        return 1
    else:
        return 0

def horizon(locID):
    return locs[locID][0]

def vertical(locID):
    return locs[locID][1]

def data_processing(data):
    udf1 = F.udf(func, IntegerType())
    udf2 = F.udf(horizon, IntegerType())
    udf3 = F.udf(vertical, IntegerType())
    #构建特征
    data = data.withColumn('near', udf1(data['pickupid'], data['dropoffid']))
    data = data.withColumn('time',data['duration']/60)
    data = data.withColumn('h_pu', udf2(data['pickupid']))
    data = data.withColumn('h_do', udf2(data['dropoffid']))
    data = data.withColumn('v_pu', udf3(data['pickupid']))
    data = data.withColumn('v_do', udf3(data['dropoffid']))
    #将特征封装为多维向量
    df_assember = VectorAssembler(
        inputCols=[i for i in data.columns if i not in ['_id', 'duration', 'pickup', 'dropof','pickupid','dropoffid','cong']],
        outputCol='features')
    df = df_assember.transform(data)
    return df

def model():
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.8'
    input_uri = "mongodb://127.0.0.1:taxidata.model"
    output_uri = "mongodb://127.0.0.1:spark.spark_result"
    #连接spark
    my_spark = SparkSession\
        .builder\
        .appName("MyApp")\
        .config("spark.mongodb.input.uri", input_uri)\
        .config("spark.mongodb.output.uri", output_uri)\
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.0')\
        .getOrCreate()
    #读入数据，数据处理
    data = my_spark.read.format("mongo").option("uri","mongodb://127.0.0.1/taxidata.model").load()
    train_df = data_processing(data)
    pred_df = my_spark.read.format("mongo").option("uri","mongodb://127.0.0.1/taxidata.dispatch").load()
    pred_df = data_processing(pred_df)

    #模型训练
    rf = LinearRegression(featuresCol='features', labelCol='time',maxIter=2000, regParam=0.25,elasticNetParam=0.8)
    #rf = GBTRegressor(labelCol="time", featuresCol="features",maxIter=200,subsamplingRate=0.8)
    #rf = RandomForestRegressor(featuresCol='features', labelCol='time')
    rf_model = rf.fit(train_df)
    # 预测
    rf_prediction = rf_model.transform(pred_df)
    evaluator = RegressionEvaluator(labelCol='time', predictionCol='prediction')
    # 用 MSE 和 R2 进行评估
    mse = evaluator.evaluate(rf_prediction, {evaluator.metricName: 'mse'})
    r2 = evaluator.evaluate(rf_prediction, {evaluator.metricName: 'r2'})
    print("MSE为:", mse,"R2得分：", r2)
    rf_model.save("")
    #rf_prediction.write.format("mongo").mode("overwrite").option("taxidata","dispatch").option("collection", "contacts").save()
    return rf_model

def pred():
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.8'
    input_uri = "mongodb://127.0.0.1:taxidata.model"
    output_uri = "mongodb://127.0.0.1:spark.spark_result"
    #连接spark
    my_spark = SparkSession \
        .builder \
        .appName("MyApp") \
        .config("spark.mongodb.input.uri", input_uri) \
        .config("spark.mongodb.output.uri", output_uri) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0') \
        .getOrCreate()
    #读入数据，预测结果写入mongoDB
    model = LinearRegressionModel.load('Model test/rfModel')
    data = my_spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/taxidata.dispatch").load()
    pred_df = data_processing(data)
    rf_prediction = model.transform(data)


if __name__ == '__main__':
    rf = model


