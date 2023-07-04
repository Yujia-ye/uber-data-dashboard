import pymongo
from sshtunnel import SSHTunnelForwarder

from pyspark.ml.regression import GBTRegressor
def pysparl():'''
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python36'
# load mongo data
    input_uri = "mongodb://127.0.0.1:taxidata.yellow"
    output_uri = "mongodb://127.0.0.1:spark.spark_test"

    my_spark = SparkSession\
        .builder\
        .appName("MyApp")\
        .config("spark.mongodb.input.uri", input_uri)\
        .config("spark.mongodb.output.uri", output_uri)\
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0')\
        .getOrCreate()

    df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load().hour_df.printSchema()

    gbdt = GBTRegressor(labelCol="cnt", featuresCol="features")
    gbdt_pipeline = Pipeline(stages=[vector, vIndexer, gbdt])
    gbdt_model = gbdt_pipeline.fit(train_df)
    gbdt_prediction = gbdt_model.transform(test_df)
    gbdt_rmse = evaluator.evaluate(gbdt_prediction)
    gbdt_rmse  # 75.863

    # 使用交叉验证找出最佳的参数
    paramGrid = ParamGridBuilder().addGrid(gbdt.maxDepth, [5, 10]).addGrid(gbdt.maxBins, [25, 40]).addGrid(gbdt.maxIter,
                                                                                                           [10, 50]).build()
    cross = CrossValidator(estimator=gbdt, evaluator=evaluator, numFolds=3, estimatorParamMaps=paramGrid, seed=1024)
    gbdt_cross = Pipeline(stages=[vector, vIndexer, cross])

    gbdt_model = gbdt_cross.fit(train_df)
    pred = gbdt_model.transform(test_df)
    rmse = evaluator.evaluate(pred)'''

HOST = '106.75.255.69'
server = SSHTunnelForwarder(
    HOST,
    ssh_username='ubuntu',
    ssh_password='yyjhx991231',
    remote_bind_address=('127.0.0.1', 27017)
)

server.start()

client = pymongo.MongoClient("127.0.0.1",server.local_bind_port ) # server.local_bind_port is assigned local port
dblist =client.list_database_names()
data = client['taxidata']['yellow']
client['taxidata']['dist'].drop()
data.aggregate([{'$match':{'trip_distance':{'$gte':0}}},
                {'$group':{'_id':{'pickup':"$PULocationID",'dropoff':"$DOLocationID"},
           'max':{'$max':"$trip_distance"},'min':{'$min':"$trip_distance"},
           'avg':{'$avg':"$trip_distance"}}},{"$out":'dist'}])


server.stop()



