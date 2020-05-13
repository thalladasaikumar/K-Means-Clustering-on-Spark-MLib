# K Means Clustering on Spark MLib
The Spark MLib ( Machine Learning Library ) with the CarEvaluation data, used K-MEANS clustering , created 3 clusters. For this created a cluster with Hadoop and Spark in AWS, used AWS S3 for input of data set and executable jar files.


## Steps to execute the project
1. Create a Mavan project in Eclipse for Spark and extract the .jar file.
2. Create a cluster with Hadoop and Spark in AWS and start the cluster. Once the cluster is running, log-in to the master node using Putty(Windows) or SSH(MAC or Linux)
3. Create a data bucket in AWS S3. Upload the Car Data .jar files to S3
4. From the master node download SparkAction-0.0.1-SNAPSHOT.jar using the command:
	_aws s3 cp s3://BUCKET_NAME/SparkAction-0.0.1-SNAPSHOT.jar ._

5. Run the .jar file using your terminal or Putty using following command:
	_spark-submit --class package_name.ClassName --master yarn --deploy-mode client LOCATION_OF_JAR_FILE s3://BUCKET_NAME/CarData.txt s3://BUCKET_NAME/KMeansOutput_
