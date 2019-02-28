# MapReduceProject
Hadoop 2.x installation guide
Version 2.7 and later of Apache Hadoop requires at least Java 7. Here we will use Hadoop 2.9.2 and Java 8. You are free to use other versions.

1.	Install Java: 
$sudo apt-get update
 	$sudo apt-get install openjdk-8-jre openjdk-8-jdk
   Set Java_Home:
$vi ~/.bashrc
   Add the following command to the file:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   Save and close the file. Activate the setting immediately with:
 	$source ~/.bashrc
2. Install Hadoop
  Download Hadoop-2.9.2.tar.gz: 
      $wget http://mirror.bit.edu.cn/apache/hadoop/common/stable2/hadoop-2.9.2.tar.gz

  Install Hadoop to /usr/local
	$ sudo tar -zxvf ./hadoop-2.9.2.tar.gz -C /usr/local 
	$ cd /usr/local/
	$ sudo mv ./hadoop-2.9.2/ ./hadoop 
	$ sudo chown -R username:username ./hadoop  (This command is to change permission of hadoop file. Replace the username with your account username. For example, my account username is Fengli and the command will be like: $ sudo chown -R fengli:fengli ./hadoop )

3. Run an example of Hadoop
	$cd /usr/local/hadoop
	$mkdir input
	$cp ./etc/hadoop/*.xml input
	$./bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep input output 'dfs[a-z.]+'
	$cat ./output/part-r-00000
	$rm -R ./output   (delete output)
Run MapReduce "WordCount" program 
1. Add CLASSPATH to bashrc file.
	$ vi  ~/.bashrc

 Set the Hadoop path by adding the following commands to the file:
HADOOP_HOME=/usr/local/hadoop	
export CLASSPATH=$HADOOP_HOME/share/hadoop/common/hadoop-common-2.9.2.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$CLASSPATH

 Activate the path setting immediately:	
$source ~/.bashrc 

2. Compile WordCount program(ignore the Warnings)
	$javac WordCount.java -xlint:deprecation

3. Make a jar file
   $jar -cvf WordCount.jar ./WordCount*.class

4. Set up input files
	$mkdir input
	$echo "echo of the rainbow" > ./input/file0
	$echo "the waiting game" > ./input/file1

5. Run the program
	$/usr/local/hadoop/bin/hadoop jar WordCount.jar WordCount input output

