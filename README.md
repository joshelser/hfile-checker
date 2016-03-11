# HFile Checker

A simple utility designed to read an Apache HBase HFile for the purposes of finding any invalid blocks.

## Building

`mvn package` will generate a fat-jar (jar-with-dependencies) that should be capable of executing standalone. Use
the options `hadoop.version` and `hbase.version` to build against a specific version of Apache Hadoop and Apache HBase
if this is important (e.g. `mvn package -Dhadoop.version=2.7.1 -Dhbase.version=1.1.4`)

## Running

`jar -cp "hfilechecker-$VERSION-jar-with-dependencies.jar:${HADOOP_CONF_DIR}" com.github.joshelser.hbase.HFileChecker
hdfs://<your_namenode_host>:8020/path/in/hdfs/to/an/hfile`
