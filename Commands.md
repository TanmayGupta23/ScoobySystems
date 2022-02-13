# Commands to Run the MapReduce Program
**\<parent directory\>** *= directory that will store Main.java, package, and jar file*

*Our setup had a user named “hadoop” - please adjust the code depending on your specific username*


<ins>*(Not always necessary) Remove old-versions of MapReduce files (Note: user should be default - with permissions to manipulate files)*</ins>
cd;cd /home/hadoop/**\<parent directory\>**;sudo rm **\<jar name\>**.jar;sudo rm -r **\<package name\>**;rm Main.java

<ins>*(Not always necessary) Move Main.java from downloads to **\<parent directory\>***</ins>

<ins>*Switch to Hadoop user*</ins>
su hadoop

<ins>*Set classpaths*</ins>
cd;cd **\<parent directory\>**;export HADOOP\_CLASSPATH="/home/hadoop/hadoop-3.3.1/etc/hadoop:/home/hadoop/hadoop-3.3.1/share/hadoop/common/lib/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/common/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/hdfs:/home/hadoop/hadoop-3.3.1/share/hadoop/hdfs/lib/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/hdfs/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/mapreduce/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/yarn:/home/hadoop/hadoop-3.3.1/share/hadoop/yarn/lib/\*:/home/hadoop/hadoop-3.3.1/share/hadoop/yarn/\*:/home/hadoop/json-simple-1.1.1.jar";export CLASSPATH="$HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.1.jar:$HADOOP\_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.3.1.jar:$HADOOP\_HOME/share/hadoop/common/hadoop-common-3.3.1.jar:~/**\<parent directory\>**/\*:$HADOOP\_HOME/lib/\*:/home/hadoop/json-simple-1.1.1.jar"

<ins>*Start up Hadoop*</ins>
$HADOOP\_HOME/sbin/start-dfs.sh;$HADOOP\_HOME/sbin/start-yarn.sh

<ins>*Create Hadoop Files*</ins>
cd /home/hadoop/**\<parent directory\>**;javac -d . Main.java;jar cfm **\<jar name\>**.jar Manifest.txt **\<package name\>**/\*.class

<ins>*Run MapReduce program*</ins>
$HADOOP\_HOME/bin/hadoop jar **\<jar name\>**.jar **\<input directory\> \<output directory\>**

<ins>*Display Output*</ins>
$HADOOP\_HOME/bin/hdfs dfs -cat /user/hadoop/**\<output directory\>**/part-r-00000

Miscellaneous commands:

<ins>*Deleting txt files from HDFS*</ins>
hdfs dfs -rm -skipTrash /user/hadoop/**\<directory\>**/**\<file name\>**.txt

<ins>*Deleting DIRECTORIES from HDFS*</ins>
hdfs dfs -rm -r -skipTrash /user/hadoop/**\<directory\>**

<ins>*Copying a txt file to HDFS  (Note: when copying a file to hdfs, be in the Hadoop user)*</ins>
$HADOOP\_HOME/bin/hdfs dfs -put /**\<path to file\>**.txt /user/hadoop/**\<directory\>**
