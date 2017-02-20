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

import java.io.File;
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
import com.amazonaws.auth.AWSCredentialsProviderChain;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.Constants.*;

public class S3AFileSystem extends FileSystem {
  private URI uri;
  private Path workingDir;
  private AmazonS3Client s3;
  private String bucket;
  private int maxKeys;
  private long partSize;
  private long partSizeThreshold;
  public static final Log LOG = LogFactory.getLog(S3AFileSystem.class);
  private CannedAccessControlList cannedACL;
  private String serverSideEncryptionAlgorithm;
  
  // The maximum number of entries that can be deleted in any call to s3
  private static final int MAX_ENTRIES_TO_DELETE = 1000;


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
    String accessKey = conf.get(NEW_ACCESS_KEY, conf.get(OLD_ACCESS_KEY, null));
    String secretKey = conf.get(NEW_SECRET_KEY, conf.get(OLD_SECRET_KEY, null));

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

    AWSCredentialsProviderChain credentials = new AWSCredentialsProviderChain(
        new BasicAWSCredentialsProvider(accessKey, secretKey),
        new InstanceProfileCredentialsProvider(),
        new AnonymousAWSCredentialsProvider()
    );

    bucket = name.getHost();

    ClientConfiguration awsConf = new ClientConfiguration();
    awsConf.setMaxConnections(conf.getInt(NEW_MAXIMUM_CONNECTIONS, conf.getInt(OLD_MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS)));
    awsConf.setProtocol(conf.getBoolean(NEW_SECURE_CONNECTIONS, conf.getBoolean(OLD_SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)) ? Protocol.HTTPS : Protocol.HTTP);
    awsConf.setMaxErrorRetry(conf.getInt(NEW_MAX_ERROR_RETRIES, conf.getInt(OLD_MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES)));
    awsConf.setSocketTimeout(conf.getInt(NEW_SOCKET_TIMEOUT, conf.getInt(OLD_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT)));

    s3 = new AmazonS3Client(credentials, awsConf);

    maxKeys = conf.getInt(NEW_MAX_PAGING_KEYS, conf.getInt(OLD_MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS));
    partSize = conf.getLong(NEW_MULTIPART_SIZE, conf.getLong(OLD_MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE));
    partSizeThreshold = conf.getLong(NEW_MIN_MULTIPART_THRESHOLD, conf.getLong(OLD_MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD));

    if (partSize < 5 * 1024 * 1024) {
      LOG.error(NEW_MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    if (partSizeThreshold < 5 * 1024 * 1024) {
      LOG.error(NEW_MIN_MULTIPART_THRESHOLD + " must be at least 5 MB");
      partSizeThreshold = 5 * 1024 * 1024;
    }

    String cannedACLName = conf.get(NEW_CANNED_ACL, conf.get(OLD_CANNED_ACL, DEFAULT_CANNED_ACL));
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }

    if (!s3.doesBucketExist(bucket)) {
      throw new IOException("Bucket " + bucket + " does not exist");
    }

    boolean purgeExistingMultipart = conf.getBoolean(NEW_PURGE_EXISTING_MULTIPART, conf.getBoolean(OLD_PURGE_EXISTING_MULTIPART, DEFAULT_PURGE_EXISTING_MULTIPART));
    long purgeExistingMultipartAge = conf.getLong(NEW_PURGE_EXISTING_MULTIPART_AGE, conf.getLong(OLD_PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE));

    if (purgeExistingMultipart) {
      TransferManager transferManager = new TransferManager(s3);
      Date purgeBefore = new Date(new Date().getTime() - purgeExistingMultipartAge*1000);

      transferManager.abortMultipartUploads(bucket, purgeBefore);
      transferManager.shutdownNow(false);
    }

    serverSideEncryptionAlgorithm = conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM, null);
    
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

    LOG.info("Opening '" + f + "' for reading");
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new IOException("Can't open " + f + " because it is a directory");
    }

    return new FSDataInputStream(new S3AInputStream(bucket, pathToKey(f), fileStatus.getLen(), s3, statistics));
  }

  /**
   * Get the close status of a file
   * @param src The path to the file
   *
   * @return return true if file is closed
   * @throws FileNotFoundException if the file does not exist.
   * @throws IOException If an I/O error occurred
   */
  public boolean isFileClosed(final Path src) throws IOException {
    return true;
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

    // We pass null to FSDataOutputStream so it won't count writes that are being buffered to a file
    return new FSDataOutputStream(new S3AOutputStream(getConf(), s3, this, bucket, key, progress, cannedACL, statistics, serverSideEncryptionAlgorithm), null);
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
   * Warning: S3 does not support renames. This method does a copy which can take S3 some time to execute with large
   *          files and directories. Since there is no Progressable passed in, this can time out jobs.
   *
   * Note: This implementation differs with other S3 drivers. Specifically:
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
    LOG.info("Rename path " + src + " to " + dst);

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

    S3AFileStatus dstStatus = null;
    try {
      dstStatus = getFileStatus(dst);

      if (srcStatus.isFile() && dstStatus.isDirectory()) {
        LOG.info("rename: src is a file and dst is a directory");
        return false;
      }

      if (srcStatus.isDirectory() && dstStatus.isFile()) {
        LOG.info("rename: src is a directory and dst is a file");
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
          S3AFileStatus dstParentStatus = getFileStatus(dst.getParent());
          if (!dstParentStatus.isDirectory()) {
            return false;
          }
        } catch (FileNotFoundException e2) {
          return false;
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: renaming file " + src + " to " + dst);
      }
      copyFile(srcKey, dstKey);
      delete(src, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rename: renaming directory " + src + " to " + dst);
      }

      // This is a directory to directory copy
      if (!dstKey.endsWith("/")) {
        dstKey = dstKey + "/";
      }

      if (!srcKey.endsWith("/")) {
        srcKey = srcKey + "/";
      }

      List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<DeleteObjectsRequest.KeyVersion>();
      if (dstStatus != null && dstStatus.isEmptyDirectory()) {
        copyFile(srcKey, dstKey);
        statistics.incrementWriteOps(1);
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(srcKey));
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      request.setMaxKeys(maxKeys);

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          keysToDelete.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
          String newDstKey = dstKey + summary.getKey().substring(srcKey.length());
          copyFile(summary.getKey(), newDstKey);
          
          if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
          	DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
          	s3.deleteObjects(deleteRequest);
          	statistics.incrementWriteOps(1);
          	keysToDelete.clear();
          }
        }

        if (objects.isTruncated()) {
          objects = s3.listNextBatchOfObjects(objects);
          statistics.incrementReadOps(1);
        } else {
          break;
        }
      }


      if (!keysToDelete.isEmpty()) {
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
        deleteRequest.setKeys(keysToDelete);
        s3.deleteObjects(deleteRequest);
        statistics.incrementWriteOps(1);
      }
    }

    if (src.getParent() != dst.getParent()) {
      deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(src.getParent());
    }
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
    LOG.info("Delete path " + f + " - recursive " + recursive);
    S3AFileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't delete " + f + " - does not exist");
      }
      return false;
    }

    String key = pathToKey(f);

    if (status.isDirectory()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("delete: Path is a directory");
      }

      if (!recursive) {
        throw new IOException("Path is a folder: " + f);
      }

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (status.isEmptyDirectory()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deleting fake empty directory");
        }
        s3.deleteObject(bucket, key);
        statistics.incrementWriteOps(1);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Getting objects for directory prefix " + key + " to delete");
        }

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);
        // Hopefully not setting a delimiter will cause this to find everything
        //request.setDelimiter("/");
        request.setMaxKeys(maxKeys);

        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        ObjectListing objects = s3.listObjects(request);
        statistics.incrementReadOps(1);
        while (true) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got object to delete " + summary.getKey());
            }
            
            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
            	DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
            	s3.deleteObjects(deleteRequest);
            	statistics.incrementWriteOps(1);
            	keys.clear();
            }
          }


          if (objects.isTruncated()) {
            objects = s3.listNextBatchOfObjects(objects);
            statistics.incrementReadOps(1);
          } else {
            break;
          }
        }
        
        if (!keys.isEmpty()) {
	        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
	      	s3.deleteObjects(deleteRequest);
	      	statistics.incrementWriteOps(1);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("delete: Path is a file");
      }
      s3.deleteObject(bucket, key);
      statistics.incrementWriteOps(1);
    }

    createFakeDirectoryIfNecessary(f.getParent());

    return true;
  }

  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new fake directory at " + f);
      }
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
    LOG.info("List status for path: " + f);

    final List<FileStatus> result = new ArrayList<FileStatus>();
    final FileStatus fileStatus =  getFileStatus(f);

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + "/";
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(maxKeys);

      if (LOG.isDebugEnabled()) {
        LOG.debug("listStatus: doing listObjects for directory " + key);
      }

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          Path keyPath = keyToPath(summary.getKey()).makeQualified(uri, workingDir);
          // Skip over keys that are ourselves and old S3N _$folder$ files
          if (keyPath.equals(f) || summary.getKey().endsWith(S3N_FOLDER_SUFFIX)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring: " + keyPath);
            }
            continue;
          }

          if (objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
            result.add(new S3AFileStatus(true, true, keyPath));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: fd: " + keyPath);
            }
          } else {
            result.add(new S3AFileStatus(summary.getSize(), dateToLong(summary.getLastModified()), keyPath));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: fi: " + keyPath);
            }
          }
        }

        for (String prefix : objects.getCommonPrefixes()) {
          Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
          if (keyPath.equals(f)) {
            continue;
          }
          result.add(new S3AFileStatus(true, false, keyPath));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding: rd: " + keyPath);
          }
        }

        if (objects.isTruncated()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("listStatus: list truncated - getting next batch");
          }

          objects = s3.listNextBatchOfObjects(objects);
          statistics.incrementReadOps(1);
        } else {
          break;
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding: rd (not a dir): " + f);
      }
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
    LOG.info("Making directory: " + f);

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

    LOG.info("Getting path status for " + f + " (" + key + ")");

    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
        statistics.incrementReadOps(1);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found exact file: fake directory");
          }
          return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found exact file: normal file");
          }
          return new S3AFileStatus(meta.getContentLength(), dateToLong(meta.getLastModified()),
              f.makeQualified(uri, workingDir));
        }
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() != 404) {
          printAmazonServiceException(e);
          throw e;
        }
      } catch (AmazonClientException e) {
        printAmazonClientException(e);
        throw e;
      }

      // Necessary?
      if (!key.endsWith("/")) {
        try {
          String newKey = key + "/";
          ObjectMetadata meta = s3.getObjectMetadata(bucket, newKey);
          statistics.incrementReadOps(1);

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found file (with /): fake directory");
            }
            return new S3AFileStatus(true, true, f.makeQualified(uri, workingDir));
          } else {
            LOG.warn("Found file (with /): real file? should not happen: " + key);

            return new S3AFileStatus(meta.getContentLength(), dateToLong(meta.getLastModified()),
                f.makeQualified(uri, workingDir));
          }
        } catch (AmazonServiceException e) {
          if (e.getStatusCode() != 404) {
            printAmazonServiceException(e);
            throw e;
          }
        } catch (AmazonClientException e) {
          printAmazonClientException(e);
          throw e;
        }
      }
    }

    try {
      if (!key.isEmpty() && !key.endsWith("/")) {
        key = key + "/";
      }
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(1);

      ObjectListing objects = s3.listObjects(request);
      statistics.incrementReadOps(1);

      if (objects.getCommonPrefixes().size() > 0 || objects.getObjectSummaries().size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as directory (with /): " + objects.getCommonPrefixes().size() + "/" + objects.getObjectSummaries().size());

          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            LOG.debug("Summary: " + summary.getKey() + " " + summary.getSize());
          }
          for (String prefix : objects.getCommonPrefixes()) {
            LOG.debug("Prefix: " + prefix);
          }
        }

        return new S3AFileStatus(true, false, f.makeQualified(uri, workingDir));
      }
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() != 404) {
        printAmazonServiceException(e);
        throw e;
      }
    } catch (AmazonClientException e) {
      printAmazonClientException(e);
      throw e;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Not Found: " + f);
    }
    throw new FileNotFoundException("No such file or directory: " + f);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5. Sadly this doesn't seem to be
   * used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    String key = pathToKey(dst);

    if (!overwrite && exists(dst)) {
      throw new IOException(dst + " already exists");
    }

    LOG.info("Copying local file from " + src + " to " + dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(partSizeThreshold);

    TransferManager transfers = new TransferManager(s3);
    transfers.setConfiguration(transferConfiguration);

    final ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
    	om.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, srcfile);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(om);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventCode()) {
          case ProgressEvent.PART_COMPLETED_EVENT_CODE:
            statistics.incrementWriteOps(1);
            break;
        }
      }
    };

    Upload up = transfers.upload(putObjectRequest);
    up.addProgressListener(progressListener);
    try {
      up.waitForUploadResult();
      statistics.incrementWriteOps(1);
    } catch (InterruptedException e) {
      throw new IOException("Got interrupted, cancelling");
    } finally {
      transfers.shutdownNow(false);
    }

    // This will delete unnecessary fake parent directories
    finishedWrite(key);

    if (delSrc) {
      local.delete(src, false);
    }
  }
  
  /**
   * Override getCanonicalServiceName because we don't support token in S3A
   */
  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
  	return null;
  }

  private void copyFile(String srcKey, String dstKey) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("copyFile " + srcKey + " -> " + dstKey);
    }

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMultipartCopyPartSize(partSize);

    TransferManager transfers = new TransferManager(s3);
    transfers.setConfiguration(transferConfiguration);

    ObjectMetadata srcom = s3.getObjectMetadata(bucket, srcKey);
    final ObjectMetadata dstom = srcom.clone();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
    	dstom.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    
    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
    copyObjectRequest.setCannedAccessControlList(cannedACL);
    copyObjectRequest.setNewObjectMetadata(dstom);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventCode()) {
          case ProgressEvent.PART_COMPLETED_EVENT_CODE:
            statistics.incrementWriteOps(1);
            break;
        }
      }
    };

    Copy copy = transfers.copy(copyObjectRequest);
    copy.addProgressListener(progressListener);
    try {
      copy.waitForCopyResult();
      statistics.incrementWriteOps(1);
    } catch (InterruptedException e) {
      throw new IOException("Got interrupted, cancelling");
    } finally {
      transfers.shutdownNow(false);
    }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting fake directory " + key + "/");
          }
          s3.deleteObject(bucket, key + "/");
          statistics.incrementWriteOps(1);
        }
      } catch (FileNotFoundException e) {
      } catch (AmazonServiceException e) {}

      if (f.isRoot()) {
        break;
      }

      f = f.getParent();
    }
  }


  private void createFakeDirectory(final String bucketName, final String objectName)
      throws AmazonClientException, AmazonServiceException {
    if (!objectName.endsWith("/")) {
      createEmptyObject(bucketName, objectName + "/");
    } else {
      createEmptyObject(bucketName, objectName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String bucketName, final String objectName)
      throws AmazonClientException, AmazonServiceException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    final ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(0L);
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
    	om.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, im, om);
    putObjectRequest.setCannedAcl(cannedACL);
    s3.putObject(putObjectRequest);
    statistics.incrementWriteOps(1);
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
