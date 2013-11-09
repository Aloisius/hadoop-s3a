An AWS SDK-backed FileSystem driver for Hadoop
==============================================

This is an experimental FileSystem for Hadoop that uses the AWS SDK 
instead of jets3t to connect. This has not been heavily tested yet. Use 
at your own risk. 

Features:

- File upload support for files > 5 GB
- Parallel copy (rename) for faster workloads
- S3 explorer compatible empty directories (no xyz_$folder$ files)


Build Instructions
------------------

Build or download aws-java-sdk 1.6.5

copy aws-java-sdk-1.6.5.jar and hadoop-s3a.jar to your hadoop classpath 
(run hadoop classpath for the appropriate directory)

Build src

```shell
$ javac -classpath `hadoop classpath` `find src/main -name '*.java'`
$ jar cvf hadoop-s3a.jar -C src/main/java .
```

Add the following keys to your core-site.xml file:

```xml
<property>
  <name>fs.s3a.awsAccessKeyId</name>
  <value>...</value>
</property>

<property>
  <name>fs.s3a.awsSecretAccessKey</name>
  <value>...</value>
</property>

<property>
  <name>fs.s3a.buffer.dir</name>
  <value>${hadoop.tmp.dir}/s3a</value>
</property>

<property>
  <name>fs.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>
```

You probably want to add this to your log4j.properties file:

```ini
log4j.logger.com.amazonaws=ERROR
```
You should now be able to run commands:

```shell
$ hadoop fs -ls s3a://bucketname/foo
```


