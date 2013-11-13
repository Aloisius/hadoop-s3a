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
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hadoop.fs.s3a.S3AConstants.*;

public class S3AOutputStream extends OutputStream {
  private OutputStream backupStream;
  private File backupFile;
  private boolean closed;
  private String key;
  private String bucket;
  private AmazonS3Client client;
  private Progressable progress;
  private long partSize;
  private int partSizeThreshold;
  private S3AFileSystem fs;
  private CannedAccessControlList cannedACL;

  public static final Log LOG = S3AFileSystem.LOG;

  public S3AOutputStream(Configuration conf, AmazonS3Client client, S3AFileSystem fs, String bucket, String key,
                         Progressable progress, int bufferSize, CannedAccessControlList cannedACL)
      throws IOException {
    this.bucket = bucket;
    this.key = key;
    this.client = client;
    this.progress = progress;
    this.fs = fs;
    this.cannedACL = cannedACL;

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

    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
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
      TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
      transferConfiguration.setMinimumUploadPartSize(partSize);
      transferConfiguration.setMultipartUploadThreshold(partSizeThreshold);

      TransferManager transfers = new TransferManager(client);
      transfers.setConfiguration(transferConfiguration);

      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, backupFile);
      putObjectRequest.setCannedAcl(cannedACL);

      Upload up = transfers.upload(putObjectRequest);
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
