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

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.hadoop.fs.s3a.S3AConstants.*;

public class S3AOutputStream extends OutputStream {
  private OutputStream backupStream;
  private File backupFile;
  private MessageDigest digest;
  private boolean closed;
  private String key;
  private String bucket;
  private AmazonS3Client client;
  private Progressable progress;
  private long partSize;
  private int partSizeThreshold;
  private S3AFileSystem fs;

  public static final Log LOG = S3AFileSystem.LOG;

  public S3AOutputStream(Configuration conf, AmazonS3Client client, S3AFileSystem fs, String bucket, String key,
                         Progressable progress, int bufferSize)
      throws IOException {
    this.bucket = bucket;
    this.key = key;
    this.client = client;
    this.progress = progress;
    this.fs = fs;

    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    partSizeThreshold = conf.getInt(MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

    File dir = new File(conf.get(BUFFER_DIR, conf.get("fs.s3.buffer.dir")));

    if (!dir.mkdirs() && !dir.exists()) {
      throw new IOException("Cannot create S3 buffer directory: " + dir);
    }

    backupFile = File.createTempFile("output-", ".tmp", dir);
    backupFile.deleteOnExit();
    closed = false;

    LOG.info("OutputStream for key '" + key + "' writing to tempfile '" + this.backupFile + "'");

    try {
      digest = MessageDigest.getInstance("MD5");
      this.backupStream = new BufferedOutputStream(new DigestOutputStream(
          new FileOutputStream(backupFile), digest));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Cannot load MD5 digest algorithm, skipping message integrity check.", e);
      this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    }
  }

  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    backupStream.close();
    LOG.info("OutputStream for key '" + key + "' closed. Now beginning upload");

    try {
      ObjectMetadata metaData = new ObjectMetadata();
      metaData.setContentLength(backupFile.length());
      if (digest != null) {
        Base64 base64 = new Base64(0);
        metaData.setContentMD5(base64.encodeToString(digest.digest()));
      }

      TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
      transferConfiguration.setMinimumUploadPartSize(partSize);
      transferConfiguration.setMultipartUploadThreshold(partSizeThreshold);

      TransferManager transfers = new TransferManager(client);
      transfers.setConfiguration(transferConfiguration);

      Upload up = transfers.upload(bucket, key, new BufferedInputStream(new FileInputStream(backupFile)), metaData);
      up.addProgressListener(new ProgressableProgressListener((progress)));

      up.waitForUploadResult();

      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (!backupFile.delete()) {
        LOG.warn("Could not delete temporary s3a file: " + backupFile);
      }
      super.close();
      closed = true;
    }

    LOG.info("OutputStream for key '" + key + "' upload complete");
  }

  @Override
  public void write(int b) throws IOException {
    backupStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    backupStream.write(b, off, len);
  }

  class ProgressableProgressListener implements ProgressListener {
    private Progressable progress;

    ProgressableProgressListener(Progressable progress) {
      this.progress = progress;
    }

    public void progressChanged(ProgressEvent progressEvent) {
      if (progress != null) {
        progress.progress();
      }
    }
  }
}
