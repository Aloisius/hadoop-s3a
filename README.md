An AWS SDK-backed FileSystem driver for Hadoop
==============================================

This is an experimental FileSystem for Hadoop that uses the AWS SDK 
instead of jets3t to connect. It is intended as a replacement for the 
s3native FileSystem. This has not been heavily tested yet. Use at your 
own risk.

Features:

- File upload & copy support for files >5 GB
- Significantly faster performance, especially for large files
- Parallel upload support
- Parallel copy (rename)
- AWS S3 explorer compatible empty directories using xyz/ instead of xyz_$folder$
- Ignores _$folder$ files created by s3native and other S3 browsing utilities


Build Instructions
------------------

Build or download aws-java-sdk 1.6.5

copy aws-java-sdk-1.6.5.jar, httpcore-4.2.jar, httpclient-4.2.3.jar 
(from aws lib) and hadoop-s3a.jar to your hadoop classpath (run hadoop 
classpath for the appropriate directory)

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

- fs.s3a.maxConnections - Controls how many parallel connections HttpClient spawns (default: 50)
- fs.s3a.secureConnections - Enables or disables SSL connections to S3 (default: true)
- fs.s3a.maxErrorRetries - How many times we should retry commands on transient errors (default: 10)
- fs.s3a.socketTimeout - Socket connect timeout (default: 5000)
- fs.s3a.maxPagingKeys - How many keys to request from S3 when doing directory listings at a time (default: 5000)
- fs.s3a.multipartSize - How big (in bytes) to split a upload or copy operation up into (default: 10485760)
- fs.s3a.minMultipartSize - Until a file is this large (in bytes), use non-parallel upload/copy (default: 20971520)
- fs.s3a.cannedACL - Set a canned ACL on newly created/copied objects (private | public-read | public-read-write | authenticated-read | log-delivery-write | bucket-owner-read | bucket-owner-full-control)
- fs.s3a.purgeExistingMultiPart - True if you want to purge existing multipart uploads that may not have been completed/aborted correctly (default: false)
- fs.s3a.purgeExistingMultiPartAge - Minimum age in seconds of multipart uploads to purge (default: 86400)

Caveats
-------

Hadoop uses a standard output committer which uploads files as 
filename._COPYING_ before renaming them. This can cause unnecessary 
performance issues with S3 because it does not have a rename operation 
and S3 verifies uploads against an md5 that the driver sets on the 
upload request. While this FileSystem should be significantly faster 
than the built-in s3native driver because of parallel copy support, you 
may want to consider setting a null output committer on our jobs to 
further improve performance.

Because S3 requires the file length be known before a file is uploaded, 
all output is buffered out to a temporary file first similar to the 
s3native driver.

Due to the lack of native rename() for S3, renaming extremely large 
files or directories make take a while. Unfortunately, there is no way 
to notify hadoop that progress is still being made for rename 
operations, so your job may time out unless you increase the task 
timeout.

This driver will fully ignore _$folder$ files. This was necessary so 
that it could interoperate with repositories that have had the s3native 
driver used on them.

This has only been run under CDH 4, but it should work with other 
distributions of hadoop. Be sure to watch out for conflicting versions 
of httpclient.

Statistics for the filesystem may be calcualted differently than other 
filesystems. When uploading a file, we do not count writing the 
temporary file on the local filesystem towards the local filesystem's 
written bytes count. When renaming files, we do not count the S3->S3 
copy as read or write operations. Unlike the s3native driver, we only 
count bytes written when we start the upload (as opposed to the write 
calls to the temporary local file). The driver also counts read & write 
ops, but they are done mostly to keep from timing out on large s3 
operations.

This is currently implemented as a FileSystem and not a 
AbstractFileSystem.
