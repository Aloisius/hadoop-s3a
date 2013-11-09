/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.S3AConstants.*;

public class S3AFileSystem extends FileSystem {
  private URI uri;
  private Path workingDir;
  private AWSCredentials credentials;
  private AmazonS3Client s3;
  private String bucket;
  private int maxKeys;
  private long partSize;
  private int partSizeThreshold;
  public static final Log LOG = LogFactory.getLog(S3AFileSystem.class);

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this.uri,
        this.getWorkingDirectory());

    // Try to get our credentials or just connect anonymously
    String accessKey = conf.get(ACCESS_KEY, null);
    String secretKey = conf.get(SECRET_KEY, null);

    String userInfo = name.getUserInfo();
    if (userInfo != null) {
      int index = userInfo.indexOf(':');
      if (index != -1) {
        accessKey = userInfo.substring(0, index);
        secretKey = userInfo.substring(index + 1);
      } else {
        accessKey = userInfo;
      }
    }

    if (accessKey != null && secretKey != null) {
      LOG.info("Using accessKey");
      credentials = new BasicAWSCredentials(accessKey, secretKey);
    } else {
      LOG.info("Using anonymous credentials");
      credentials = new AnonymousAWSCredentials();
    }

    bucket = name.getHost();
    LOG.info("Bucket set to " + bucket);

    ClientConfiguration awsConf = new ClientConfiguration();
    awsConf.setMaxConnections(conf.getInt(MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS));
    awsConf.setProtocol(conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS) ? Protocol.HTTPS : Protocol.HTTP);
    awsConf.setMaxErrorRetry(conf.getInt(MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES));
    awsConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT));

    s3 = new AmazonS3Client(credentials, awsConf);

    maxKeys = conf.getInt(MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);
    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    partSizeThreshold = conf.getInt(MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);
    setConf(conf);
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "s3a"
   */
  public String getScheme() {
    return "s3a";
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  public URI getUri() {
    return uri;
  }


  public S3AFileSystem() {
    super();
  }

  /* Turns a path (relative or otherwise) into an S3 key
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {

    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new IOException("Can't open " + f + " because it is a directory");
    }

    S3Object obj = s3.getObject(bucket, pathToKey(f));
    return new FSDataInputStream(new S3AInputStream(obj, s3, statistics));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    String key = pathToKey(f);

    if (!overwrite && exists(f)) {
      throw new IOException(f + " already exists");
    }

    return new FSDataOutputStream(new S3AOutputStream(getConf(), s3, this, bucket, key, progress, bufferSize), statistics);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }


  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * @warning S3 does not support renames. This method does a copy which can take S3 some time to execute with large
   *          files and directories. Since there is no Progressable passed in, this can time out jobs.
   *
   * @note This implementation differs with other S3 drivers. Specifically:
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename: rename " + src + " to " + dst);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.length() == 0 || dstKey.length() == 0) {
      LOG.info("rename: src or dst are empty");
      return false;
    }

    if (srcKey.equals(dstKey)) {
      LOG.info("rename: src and dst refer to the same file");
      return true;
    }

    S3AFileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException e) {
      LOG.info("rename: src not found " + src);
      return false;
    }


    try {
      S3AFileStatus dstStatus = getFileStatus(dst);

      if (srcStatus.isFile() && dstStatus.isDirectory()) {
        return false;
      }

      if (srcStatus.isDirectory() && dstStatus.isFile()) {
        return false;
      }

      if (dstStatus.isDirectory() && !dstStatus.isEmptyDirectory()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          S3AFileStatus dstStatus = getFileStatus(dst.getParent());
          if (!dstStatus.isDirectory()) {
            return false;
          }
        } catch (FileNotFoundException e2) {
          return false;
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.info("rename: renaming file " + src + " to " + dst);
      copyFile(srcKey, dstKey);
      delete(src, false);
    } else {
      LOG.info("rename: renaming directory " + src + " to " + dst);

      // This is a directory to directory copy
      if (!dstKey.endsWith("/")) {
        dstKey = dstKey + "/";
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      // Hopefully not setting a delimiter will cause this to find everything
      // request.setDelimiter("/");
      request.setMaxKeys(maxKeys);

      List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<DeleteObjectsRequest.KeyVersion>();
      ObjectListing objects = s3.listObjects(request);
      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          keysToDelete.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
          String newDstKey = dstKey + summary.getKey().substring(srcKey.length() + 1);
          LOG.info("rename: copyFile " + summary.getKey() + " to " + newDstKey);
          copyFile(summary.getKey(), newDstKey);
        }

        if (objects.isTruncated()) {
          objects = s3.listNextBatchOfObjects(objects);
        } else {
          break;
        }
      }

      DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
      deleteRequest.setKeys(keysToDelete);
      s3.deleteObjects(deleteRequest);
    }

    deleteUnnecessaryFakeDirectories(dst.getParent());
    createFakeDirectoryIfNecessary(src.getParent());
    return true;
  }

  /** Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.info("delete: Delete called with " + f + " - recursive " + recursive);
    S3AFileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      LOG.info("Couldn't delete " + f + " - does not exist");
      return false;
    }

    String key = pathToKey(f);

    if (status.isDirectory()) {
      LOG.info("delete: Path is a directory");
      if (!recursive) {
        throw new IOException("Path is a folder: " + f);
      }

      LOG.info("delete: Going to delete directory" + f);
      if (status.isEmptyDirectory()) {
        if (!key.endsWith("/")) {
          key = key + "/";
        }
        LOG.info("Deleting fake empty directory");
        s3.deleteObject(bucket, key);
      } else {
        LOG.info("Getting objects for prefix " + key);

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);
        // Hopefully not setting a delimiter will cause this to find everything
        //request.setDelimiter("/");
        request.setMaxKeys(maxKeys);

        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        ObjectListing objects = s3.listObjects(request);
        while (true) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            LOG.info("Got object " + summary.getKey());
          }

          DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
          deleteRequest.setKeys(keys);
          s3.deleteObjects(deleteRequest);
          keys.clear();

          if (objects.isTruncated()) {
            objects = s3.listNextBatchOfObjects(objects);
          } else {
            break;
          }
        }
      }
    } else {
      LOG.info("delete: Path is a file");
      s3.deleteObject(bucket, key);
    }

    createFakeDirectoryIfNecessary(f.getParent());

    return true;
  }

  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      LOG.info("Creating new fake directory at " + f);
      createFakeDirectory(bucket, key);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    String key = pathToKey(f);
    LOG.info("listStatus: " + f);

    final List<FileStatus> result = new ArrayList<FileStatus>();
    final FileStatus fileStatus =  getFileStatus(f);

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + "/";
      }

      LOG.info("listStatus: doing listObjects for directory " + key);
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(maxKeys);

      ObjectListing objects = s3.listObjects(request);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          Path keyPath = keyToPath(summary.getKey()).makeQualified(uri, workingDir);
          if (keyPath.equals(f)) {
            continue;
          }

          if (objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
            result.add(new S3AFileStatus(true, true, keyPath));
            LOG.info("Adding: fd: " + keyPath);
          } else {
            result.add(new S3AFileStatus(summary.getSize(), dateToLong(summary.getLastModified()), keyPath));
            LOG.info("Adding: fi: " + keyPath);
          }
        }

        for (String prefix : objects.getCommonPrefixes()) {
          Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
          if (keyPath.equals(f)) {
            continue;
          }
          result.add(new S3AFileStatus(true, false, keyPath));
          LOG.info("Adding: rd: " + keyPath);
        }

        if (objects.isTruncated()) {
          objects = s3.listNextBatchOfObjects(objects);
        } else {
          break;
        }
      }
    } else {
      LOG.info("listStatus: not a directory, just adding main fileStatus");
      result.add(fileStatus);
    }

    return result.toArray(new FileStatus[result.size()]);
  }



  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param new_dir
   */
  public void setWorkingDirectory(Path new_dir) {
    workingDir = new_dir;
  }

  /**
   * Get the current working directory for the given file system
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   * @param f path to create
   * @param permission to apply to f
   */
  // TODO: If we have created an empty file at /foo/bar and we then call mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    LOG.info("mkdirs: " + f);
    try {
      FileStatus fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new IOException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      String key = pathToKey(f);
      createFakeDirectory(bucket, key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws java.io.FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public S3AFileStatus getFileStatus(Path f) throws IOException {
    String key = pathToKey(f);

    LOG.info("Getting file status for " + f + " (" + key + ")");

    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = s3.getObjectMetadata(bucket, key);

        LOG.info("Found exact file");
        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
        } else {
          return new S3AFileStatus(meta.getContentLength(), dateToLong(meta.getLastModified()),
              f.makeQualified(uri, workingDir));
        }
      } catch (AmazonClientException e) {
        //
      }

      // Necessary?
      if (!key.endsWith("/")) {
        LOG.info("Testing with appended /");
        try {
          String newKey = key + "/";
          ObjectMetadata meta = s3.getObjectMetadata(bucket, newKey);
          LOG.info("Found with appended /");

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            LOG.info("Object represents a directory");
            return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
          } else {
            LOG.info("Object isn't a directory?");
            return new S3AFileStatus(meta.getContentLength(), dateToLong(meta.getLastModified()),
                f.makeQualified(uri, workingDir));
          }
        } catch (AmazonClientException e) {
          //
        }
      }
    }

    LOG.info("Looking using list");
    try {
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      //request.setDelimiter("/");
      request.setMaxKeys(1);

      ObjectListing objects = s3.listObjects(request);
      LOG.info("Request for prefix " + key + " returned " + objects.getCommonPrefixes().size() +
          " prefixes and " + objects.getObjectSummaries().size() + " summaries");
      if (objects.getCommonPrefixes().size() > 0 || objects.getObjectSummaries().size() > 0) {
        return new S3AFileStatus(true, false, f.makeQualified(uri, workingDir));
      }
    } catch (AmazonClientException e) {
      //
    }

    throw new FileNotFoundException("No such file or directory: " + f);
  }

  private void copyFile(String srcKey, String dstKey) throws IOException {
    LOG.info("copyFile " + srcKey + " -> " + dstKey);

    // It would be nice if the AWS TransferManager did this for us :/
    GetObjectMetadataRequest metadataRequest =
        new GetObjectMetadataRequest(bucket, srcKey);
    ObjectMetadata metadataResult = s3.getObjectMetadata(metadataRequest);
    long objectSize = metadataResult.getContentLength();

    if (objectSize <= partSizeThreshold) {
      s3.copyObject(bucket, srcKey, bucket, dstKey);
    } else {
      S3ACopyTransferManager copyTransfer = new S3ACopyTransferManager(s3, partSize);
      CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
      try {
        copyTransfer.copyObject(copyObjectRequest);
      } catch (InterruptedException e) {
        throw new IOException("Got interrupted while trying to copy");
      }
    }
  }

  // Helper function that constructs ETags.
  private static List<PartETag> GetETags(List<CopyPartResult> responses) {
    List<PartETag> etags = new ArrayList<PartETag>();
    for (CopyPartResult response : responses) {
      etags.add(new PartETag(response.getPartNumber(), response.getETag()));
    }
    return etags;
  }


  private boolean objectRepresentsDirectory(final S3ObjectSummary os) {
    return objectRepresentsDirectory(os.getKey(), os.getSize());
  }

  private boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
  }

  // Handles null Dates that can be returned by AWS
  private static long dateToLong(final Date date) {
    if (date == null) {
      return 0L;
    }

    return date.getTime();
  }

  public void finishedWrite(String key) throws IOException {
    deleteUnnecessaryFakeDirectories(keyToPath(key).getParent());
  }

  private void deleteUnnecessaryFakeDirectories(Path f) throws IOException {
    while (true) {
      try {
        String key = pathToKey(f);
        if (key.isEmpty()) {
          break;
        }

        S3AFileStatus status = getFileStatus(f);

        if (status.isDirectory() && status.isEmptyDirectory()) {
          LOG.info("Deleting fake directory " + key + "/");
          s3.deleteObject(bucket, key + "/");
        }
      } catch (AmazonServiceException e) {}

      f = f.getParent();
    }
  }


  private void createFakeDirectory(final String bucketName, final String objectName) {
    if (!objectName.endsWith("/")) {
      createEmptyObject(bucketName, objectName + "/");
    } else {
      createEmptyObject(bucketName, objectName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String bucketName, final String objectName) {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    final ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(0L);

    s3.putObject(bucketName, objectName, im, om);
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong("fs.s3a.block.size", 32 * 1024 * 1024);
  }

  private void printAmazonServiceException(AmazonServiceException ase) {
    LOG.info("Caught an AmazonServiceException, which means your request made it " +
        "to Amazon S3, but was rejected with an error response for some reason.");
    LOG.info("Error Message: " + ase.getMessage());
    LOG.info("HTTP Status Code: " + ase.getStatusCode());
    LOG.info("AWS Error Code: " + ase.getErrorCode());
    LOG.info("Error Type: " + ase.getErrorType());
    LOG.info("Request ID: " + ase.getRequestId());
    LOG.info("Class Name: " + ase.getClass().getName());
  }

  private void printAmazonClientException(AmazonClientException ace) {
    LOG.info("Caught an AmazonClientException, which means the client encountered " +
        "a serious internal problem while trying to communicate with S3, " +
        "such as not being able to access the network.");
    LOG.info("Error Message: " + ace.getMessage());
  }
}
