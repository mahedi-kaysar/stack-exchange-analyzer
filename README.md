# StackExchangeAnalyser
The project analyses the datasets of stack-exchange web portal. The datasets are downloaded as CSV formats. This project has been tested in windows host machine and centOS virtualbox.

# Hadoop Cluster Configuration 		
	1) Download and install Vagrant and VirtualBox
	2) GitBash client and Sublime text editor (for windows)
	3) git clone https://github.com/mahedi-kaysar/bigdata-pipeline-vagrant-virtualbox.git
	3) Go to the project directory using GitBash terminal
	4) vagrant up
	5) vagrant ssh
	6) sudo -s
	5) please follow the README.md file if you are facing any problem to run the cluster
	
# Development Environment Setup
	1) Open gitbash terminal in a new window.
	2) git clone https://github.com/mahedi-kaysar/stack-exchange-analyzer.git
	3) mvn clean package
	4) copy the target/original-stack-exchange-analyser-0.0.1-SNAPSHOT.jar file to bigdata-pipeline-vagrant-virtualbox\examples\stackexchange\jar
	5) keep the stack-exchange dataset inside the bigdata-pipeline-vagrant-virtualbox\examples\stackexchange\datasets directory.
	
# Run ETL for dataset cleaning	
	1) vagrant ssh
	2) sudo -s
	3) vi /vagrant/scripts/stackexchangeETL.sh (inside virtual box terminal)
	4) double check the jar and dataset directory, change the paths if necessary and save it
	5) run the script: /vagrant/scripts/stackexchangeETL.sh
	6) This will execute the ETL job and clean the dataset properly.
	7) The output of ETL will be saved in HDFS file defined in the script. 
	8) copy it to local or host machine as a csv file
# How to run Hive query
	
	1) if hive is already running execute
	2) hive
	3) run the bellow query for create a table
		CREATE EXTERNAL TABLE Posts (
	        Id STRING,
	        PostTypeId STRING,
	        AcceptedAnswerId STRING,
	        CreationDate STRING,
	        Score INT,
	        ViewCount INT,
	        Body STRING,
	        OwnerUserId STRING,
	        LastEditorUserId STRING,
	        LastEditDate STRING,
	        LastActivityDate STRING,
	        Title STRING,
	        Tags STRING,
	        AnswerCount INT,
	        CommentCount INT,
	        FavoriteCount INT
	    )
	    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	    LOCATION '/user/root/stackexchange-input';
    
    4) run the PostDao.java from eclipse
    5) or run from jar: java -jar org.mahedi.bigdata.AssignmentOneLauncher org.mahedi.bigdata.hive.db.dao.PostDao
    5) this will show all the query results

# Run TopN-TFIDF
	1) vagrant ssh
	2) sudo -s
	3) vi /vagrant/scripts/stackexchangeTFIDF.sh (inside virtual box terminal)
	4) double check the jar and dataset directory, change the paths if necessary and save it
	5) run the script: /vagrant/scripts/stackexchangeTFIDF.sh
	6) This will execute the ETL job and clean the dataset properly.
	7) The output of ETL will be saved in HDFS file defined in the script. 
	8) copy it to local or host machine as a csv file
	9) vi /vagrant/scripts/stackexchangeTopN-TFIDF.sh (inside virtual box terminal)
	10) change the input and output paths as needed. In this case input file should be the output file of previous step of stackexchangeTFIDF.sh
	11) run the script: /vagrant/scripts/stackexchangeTopN-TFIDF.sh
# Conclusion	

This project was for a assignment of CA675 module in DCU. 
