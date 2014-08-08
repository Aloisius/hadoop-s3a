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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.SocketException;

public class S3AInputStream extends FSInputStream {
  private long pos;
  private boolean closed;
  private S3ObjectInputStream wrappedStream;
  private S3Object wrappedObject;
  private FileSystem.Statistics stats;
  private AmazonS3Client client;
  private String bucket;
  private String key;
  private long contentLength;
  public static final Log LOG = S3AFileSystem.LOG;


  public S3AInputStream(String bucket, String key, long contentLength, AmazonS3Client client,
                        FileSystem.Statistics stats) {
    this.bucket = bucket;
    this.key = key;
    this.contentLength = contentLength;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
    this.wrappedObject = null;
    this.wrappedStream = null;
  }

  private void openIfNeeded() throws IOException {
    if (wrappedObject == null) {
      reopen(0);
    }
  }

  private synchronized void reopen(long pos) throws IOException {
    if (wrappedStream != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Aborting old stream to open at pos " + pos);
      }
      wrappedStream.abort();
    }

    LOG.info("Actually opening file " + key + " at pos " + pos);

    GetObjectRequest request = new GetObjectRequest(bucket, key);
    request.setRange(pos, contentLength-1);

    wrappedObject = client.getObject(request);
    wrappedStream = wrappedObject.getObjectContent();

    if (wrappedObject == null) {
      throw new IOException("Failed to make S3 GetObject request");
    }

    if (wrappedStream == null) {
      throw new IOException("Null IO stream");
    }

    this.pos = pos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (this.pos == pos) {
      return;
    }

    LOG.info("Reopening " +  wrappedObject.getKey() + " to seek to new offset " + (pos - this.pos));
    reopen(pos);
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

    openIfNeeded();

    int byteRead;
    try {
      byteRead = wrappedStream.read();
    } catch (SocketTimeoutException e) {
      LOG.error("Got timeout while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read();
    } catch (SocketException e) {
      LOG.error("Got socket exception while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read();
    } catch (IOException e) {
      LOG.error("Got IO exception while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read();    	
    }

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

    openIfNeeded();

    int byteRead;
    try {
      byteRead = wrappedStream.read(buf, off, len);
    } catch (SocketTimeoutException e) {
      LOG.error("Got timeout while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read(buf, off, len);
    } catch (SocketException e) {
      LOG.error("Got socket exception while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read(buf, off, len);
    } catch (IOException e) {
      LOG.error("Got IO exception while trying to read from stream, trying to recover " + e);
      reopen(pos);
      byteRead = wrappedStream.read(buf, off, len);    	
    }

    if (byteRead > 0) {
      pos += byteRead;
    }

    if (stats != null && byteRead > 0) {
      stats.incrementBytesRead(byteRead);
    }

    return byteRead;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (wrappedObject != null) {
    	wrappedObject.close();
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
