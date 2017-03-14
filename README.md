# StackExchangeAnalyzer
The project analyze the datasets of stack-exchange web portal. The datasets are downloaded as CSV formats. This project has been tested in windows host machine and centOS virtualbox.

# Prerequisite	
	The following softwares are needed to be running. You can follow my another project [here](https://github.com/mahedi-kaysar/bigdata-pipeline-vagrant-virtualbox) for setting the hadoop cluster.
	1) Download and install Vagrant and VirtualBox, 
	2) GitBash client and Sublime text editor (for windows)
	3) Go to the project directory using GitBash terminal
	4) vagrant up
	5) please follow the readme file if you are facing any problem to run the cluster
	
# How to build and run inside cluster
	1) mvn clean package
	2) copy the original-stack-exchange-analyser-0.0.1-SNAPSHOT.jar file from target folder to (bigdata-pipeline-vagrant-virtualbox\examples\stackexchange\jar)
	3) keep the dataset (for example 200Post.csv) inside the bigdata-pipeline-vagrant-virtualbox\examples\stackexchange\datasets directory.
	3) vagrant ssh
	4) sudo -s
	5) vi /vagrant/scripts/stackexchange.sh
	6) double check the jar and dataset directory and save it
	7) run the script: /vagrant/scripts/stackexchange.sh
	8) This will execute the map-reduce job and create a new CSV file for loading inside the hive table

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
    5) this will show the query results
	
# Conclusion	

This project was for a assignment of CA675 module in DCU. 
