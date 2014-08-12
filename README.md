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
- Supports multiple buffer dirs to even out IO
- Supports IAM role-based authentication
- Allows setting a default canned ACL for uploads


Build Instructions
------------------

Build using maven:

```shell
$ mvn package -DskipTests=true
```

Copy jar and various dependencies to your hadoop libs dir 
(run 'hadoop classpath' to find appropriate lib dir):

```shell
$ cp target/hadoop-s3a-0.0.5.jar \
     target/lib/aws-java-sdk-1.8.3.jar \
     target/lib/httpcore-4.2.jar \
     target/lib/httpclient-4.2.jar \
     target/lib/jackson-databind-2.1.1.jar \
     target/lib/jackson-core-2.1.1.jar \
     target/lib/jackson-annotations-2.1.1.jar \
     target/lib/joda-time-2.3.jar \
     /usr/lib/hadoop/lib/
```

Note: These are dependencies that are necessary for CDH 5 which is based on
Hadoop 2.2.0. There is a chance you'll need other dependencies for different
versions located in the target/lib dir.

Also, by default this builds against Hadoop 2.2.0. If you wish to build 
against a different version, edit the pom.xml file.

Add the following keys to your core-site.xml file:

```xml
<!-- omit for IAM role based authentication -->
<property>
  <name>fs.s3a.access.key</name>
  <value>...</value>
</property>

<!-- omit for IAM role based authentication -->
<property>
  <name>fs.s3a.secret.key</name>
  <value>...</value>
</property>

<property>
  <name>fs.s3a.buffer.dir</name>
  <value>${hadoop.tmp.dir}/s3a</value>
</property>

<!-- necessary for Hadoop to load our filesystem driver -->
<property>
  <name>fs.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>
```

You probably want to add this to your log4j.properties file:

```ini
log4j.logger.com.amazonaws=ERROR
log4j.logger.com.amazonaws.http.AmazonHttpClient=ERROR
log4j.logger.org.apache.hadoop.fs.s3a.S3AFileSystem=WARN
```
You should now be able to run commands:

```shell
$ hadoop fs -ls s3a://bucketname/foo
```


Tunable parameters
------------------

These may or may not improve performance. The defaults were set without 
much testing.

- fs.s3a.access.key - Your AWS access key ID
- fs.s3a.secret.key - Your AWS secret key
- fs.s3a.connection.maximum - Controls how many parallel connections HttpClient spawns (default: 15)
- fs.s3a.connection.ssl.enabled - Enables or disables SSL connections to S3 (default: true)
- fs.s3a.attempts.maximum - How many times we should retry commands on transient errors (default: 10)
- fs.s3a.connection.timeout - Socket connect timeout (default: 5000)
- fs.s3a.paging.maximum - How many keys to request from S3 when doing directory listings at a time (default: 5000)
- fs.s3a.multipart.size - How big (in bytes) to split a upload or copy operation up into (default: 100 MB)
- fs.s3a.multipart.threshold - Until a file is this large (in bytes), use non-parallel upload (default: 2 GB)
- fs.s3a.acl.default - Set a canned ACL on newly created/copied objects (Private | PublicRead | PublicReadWrite | AuthenticatedRead | LogDeliveryWrite | BucketOwnerRead | BucketOwnerFullControl)
- fs.s3a.multipart.purge - True if you want to purge existing multipart uploads that may not have been completed/aborted correctly (default: false)
- fs.s3a.multipart.purge.age - Minimum age in seconds of multipart uploads to purge (default: 86400)
- fs.s3a.buffer.dir - Comma separated list of directories that will be used to buffer file writes out of (default: uses fs.s3.buffer.dir)
- fs.s3a.server-side-encryption-algorithm - Name of server side encryption algorithm to use for writing files (e.g. AES256) (default: null)

Key name changes in 0.0.2
--------------------------

Many configuration keys were renamed in 0.0.2 in order to better match the
style of newer versions of hadoop. Older keys should continue to work for
the time being.

- fs.s3a.awsAccessKeyId -> fs.s3a.access.key
- fs.s3a.awsSecretAccessKey -> fs.s3a.secret.key
- fs.s3a.maxConnections -> fs.s3a.connection.maximum
- fs.s3a.secureConnections -> fs.s3a.connection.ssl.enabled
- fs.s3a.maxErrorRetries -> fs.s3a.attempts.maximum
- fs.s3a.socketTimeout -> fs.s3a.connection.timeout
- fs.s3a.maxPagingKeys -> fs.s3a.paging.maximum
- fs.s3a.multipartSize -> fs.s3a.multipart.size
- fs.s3a.minMultipartSize -> fs.s3a.multipart.threshold
- fs.s3a.cannedACL -> fs.s3a.acl.default
- fs.s3a.purgeExistingMultiPart -> fs.s3a.multipart.purge
- fs.s3a.purgeExistingMultiPartAge -> fs.s3a.multipart.purge.age

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
driver used on them, but means that it won't recognize any empty directories
that the s3native filesystem had created.

This has only been tested under CDH 4 and CDH 5, but it should work with other 
distributions of hadoop. Be sure to watch out for conflicting versions 
of httpclient.

Statistics for the filesystem may be calculated differently than other 
filesystems. When uploading a file, we do not count writing the temporary 
file on the local filesystem towards the local filesystem's written bytes 
count. When renaming files, we do not count the S3->S3 copy as read or write 
operations. Unlike the s3native driver, we only count bytes written when we 
start the upload (as opposed to the write calls to the temporary local file). 
The driver also counts read & write ops, but they are done mostly to keep 
from timing out on large s3 operations.

This is currently implemented as a FileSystem and not a AbstractFileSystem.

Changes
-------

0.0.5

- Integrated server side encryption patch from David S. Wang.
- Integrated bugfix that prevented ExportSnapshot to work with Kerberized clusters from  David S. Wang
- Integrated bugfix where we weren't reading new parameter names from David S. Wang
- Integrated bugfix to limit delete calls to 1000 objects at a time from  David S. Wang 
- Retry one time on IOException during read() calls
