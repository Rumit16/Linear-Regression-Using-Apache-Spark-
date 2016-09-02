Name - Rumit Gajera				UNCC ID: 800890584 
			Assignment -5
Step 1 - Put the input file in HDFS using -put following command

hadoop fs -put <source directory> <destination directory>
ex - 
	hadoop fs -put /users/rgajera/Spark_input/yxlin.csv /user/rgajera/spark_input

Step 2 - Go to the directory of source code using cd

Step 3 - run the code by giving the path of input as command line argument

ex -
	pyspark linearReg.py /user/rgajera/spark_input/yxlin.csv
	 
Step 4 - After running this code you can see the output on command line window like

*********OUTPUT*******
beta:
	<values>
	<values> 