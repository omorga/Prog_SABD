## Download

#### Project
Download project: git clone https://github.com/omorga/Prog_SABD.git

#### HBase
Download and configure hbase release 1.2.2 from HBase http://www.apache.org/dyn/closer.cgi/hbase/ 

#### Nifi
Download nifi release 1.2.0 from Nifi download https://nifi.apache.org/download.html

## Run

1) Create folders ratings and movies and copy the input files ratings.csv and movies.csv (download ml-20m.zip from https://grouplens.org/datasets/movielens/) in the corresponding directory.

2) Start Hadoop whit the command
  hdfs namenode -format  
	$HADOOP_HOME/sbin/start-dfs.sh
	$HADOOP_HOME/sbin/start-yarn.sh
  start Hbase with the command ./bin/start-hbase.sh

3) Once NiFi has been downloaded and installed, it can be started by using the mechanism appropriate for your operating system.(guide https://nifi.apache.org/docs/nifi-docs/html/getting-started.html#starting-nifi)

4) Inject files in hadoop file system with Nifi. NiFi has been started, to get started, open a web browser and navigate to http://localhost:8080/nifi. The port can be changed by editing the nifi.properties file in the NiFi conf directory, but the default port is 8080.

5) To import a template into your NiFi instance, select the Upload Template icon from the Operator palette, click the Search Icon and find the templante in \template_nifi\nifiTemplate. We can right-click and choose the Start menu item.

6) Retrieve jar in \out\artifacts\prog.jar

7) Run hadoop jar prog.jar query.Query1 hdfs:///data/rating.csv hdfs:///data/movie.csv hdfs:///output1
 or query.Query2 hdfs:///data/rating.csv hdfs:///data/movie.csv hdfs:///output2 hdfs:///output3
 or query.Query3 hdfs:///data/rating.csv hdfs:///data/movie.csv hhdfs:///output4
 to execute a query without Hbase

8) Run hadoop jar prog.jar queryHbase.Query1Hbase hdfs:///data/rating.csv hdfs:///data/movie.csv
  or queryHbase.Query2Hbase  hdfs:///data/rating.csv hdfs:///data/movie.csv hdfs:///output1
  or queryHbase.Query3Hbase hdfs:///data/rating.csv hdfs:///data/movie.csv
  to execute a query with Hbase






