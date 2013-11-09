An AWS SDK-backed FileSystem driver for Hadoop
==============================================

This is an experimental FileSystem for Hadoop that uses the AWS SDK 
instead of jets3t to connect. This has not been heavily tested yet. Use 
at your own risk. 

Features:

- File upload & copy support for files >5 GB
- Parallel upload support
- Parallel copy (rename)
- S3 explorer compatible empty directories using xyz/ instead of xyz_$folder$


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
log4j.logger.org.apache.hadoop.fs=ERROR
```
You should now be able to run commands:

```shell
$ hadoop fs -ls s3a://bucketname/foo
```


Tunable parameters
------------------

These may or may not improve performance. The defaults were set without 
much testing.

- fs.s3a.maxConnections - Controls how many parallel connections HttpClient spawns
- fs.s3a.secureConnections - Enables or disables SSL connections to S3
- fs.s3a.maxErrorRetries - How many times we should retry commands on transient errors
- fs.s3a.socketTimeout - Socket connect timeout
- fs.s3a.maxPagingKeys - How many keys to request from S3 when doing directory listings at a time
- fs.s3a.multipartSize - How big (in bytes) to split a upload or copy operation up into
- fs.s3a.minMultipartSize - Until a file is this large (in bytes), use non-parallel upload/copy

