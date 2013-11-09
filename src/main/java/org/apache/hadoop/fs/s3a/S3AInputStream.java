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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;

public class S3AInputStream extends FSInputStream {
  private long pos;
  private boolean closed;
  private S3ObjectInputStream wrappedStream;
  private S3Object wrappedObject;
  private FileSystem.Statistics stats;
  private AmazonS3Client client;
  public static final Log LOG = S3AFileSystem.LOG;

  public S3AInputStream(S3Object obj, AmazonS3Client client,
                        FileSystem.Statistics stats) {
    if (obj.getObjectContent() == null) {
      throw new IllegalArgumentException("Null InputStream");
    }

    this.wrappedObject = obj;
    this.wrappedStream = obj.getObjectContent();
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (this.pos == pos) {
      return;
    }

    LOG.info("Reopening " +  wrappedObject.getKey() + " to seek to new offset " + (pos - this.pos));
    wrappedStream.abort();

    ObjectMetadata metadata = wrappedObject.getObjectMetadata();

    GetObjectRequest request = new GetObjectRequest(wrappedObject.getBucketName(), wrappedObject.getKey());
    request.setRange(pos, metadata.getContentLength());
    wrappedObject = client.getObject(request);
    wrappedStream = wrappedObject.getObjectContent();
    this.pos = pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    super.close();
    closed = true;
    wrappedObject.close();
  }

  @Override
  public boolean markSupported() {
    return wrappedStream.markSupported();
  }

  @Override
  public void mark(int readLimit) {
    wrappedStream.mark(readLimit);
  }

  @Override
  public void reset() throws IOException {
    wrappedStream.reset();
  }
}
