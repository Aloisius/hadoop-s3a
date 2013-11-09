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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.s3a.S3AConstants.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

/* Tests a live S3 system. If you keys and bucket aren't specified, all tests are marked as passed */
public class TestLiveS3AFileSystem {
  private Configuration conf;
  private S3AFileSystem fileSys;
  private boolean liveTest;
  private String testBucket;
  private URI testURI;
  private String testRootDir;

  private static final int TEST_BUFFER_SIZE = 128;
  private static final int MODULUS = 128;

  public static final Log LOG = LogFactory.getLog(S3AFileSystem.class);


  @Before
  public void setup() throws IOException {
    conf = new Configuration(false);

    String accessKey = conf.get(ACCESS_KEY, null);
    String secretKey = conf.get(SECRET_KEY, null);
    testBucket = conf.get("fs.s3a.testBucket", null);
    testRootDir = "/test." + UUID.randomUUID() + "/";

    if (accessKey == null || secretKey == null || testBucket == null) {
      liveTest = false;
      return;
    }

    liveTest = true;

    testURI =  URI.create("s3a://" + testBucket + testRootDir);
    fileSys = new S3AFileSystem();
    fileSys.initialize(testURI, conf);

    // Wipe the test bucket
    /*
    fileSys.delete(new Path(testRootDir), true);
    fileSys.mkdirs(new Path(testRootDir));
    */
  }

  @After
  public void after() throws IOException {
    if (liveTest) {
      fileSys.delete(new Path(testRootDir), true);
    }
  }

  @Test(timeout = 10000)
  public void testMkdirs() throws IOException {
    if (!liveTest) {
      return;
    }

    // No trailing slash
    assertTrue(fileSys.mkdirs(new Path(testRootDir, "a")));
    assertTrue(fileSys.exists(new Path(testRootDir, "a")));

    // With trailing slash
    assertTrue(fileSys.mkdirs(new Path(testRootDir, "b/")));
    assertTrue(fileSys.exists(new Path(testRootDir, "b/")));

    // Two levels deep
    assertTrue(fileSys.mkdirs(new Path(testRootDir, "c/a/")));
    assertTrue(fileSys.exists(new Path(testRootDir, "c/a/")));

    // Mismatched slashes
    assertTrue(fileSys.exists(new Path(testRootDir, "c/a")));
  }


  @Test(timeout=20000)
  public void testDelete() throws IOException {
    if (!liveTest) {
      return;
    }

    // Test deleting an empty directory
    assertTrue(fileSys.mkdirs(new Path(testRootDir, "d")));
    assertTrue(fileSys.delete(new Path(testRootDir, "d"), true));
    assertFalse(fileSys.exists(new Path(testRootDir, "d")));

    // Test deleting a deep empty directory
    assertTrue(fileSys.mkdirs(new Path(testRootDir, "e/f/g/h")));
    assertTrue(fileSys.delete(new Path(testRootDir, "e/f/g"), true));
    assertFalse(fileSys.exists(new Path(testRootDir, "e/f/g/h")));
    assertFalse(fileSys.exists(new Path(testRootDir, "e/f/g")));
    assertTrue(fileSys.exists(new Path(testRootDir, "e/f")));

    // Test delete of just a file
    writeFile(new Path(testRootDir, "f/f/file"), 1000);
    assertTrue(fileSys.exists(new Path(testRootDir, "f/f/file")));
    assertTrue(fileSys.delete(new Path(testRootDir, "f/f/file"), false));
    assertFalse(fileSys.exists(new Path(testRootDir, "f/f/file")));


    // Test delete of a path with files in various directories
    writeFile(new Path(testRootDir, "g/h/i/file"), 1000);
    assertTrue(fileSys.exists(new Path(testRootDir, "g/h/i/file")));
    writeFile(new Path(testRootDir, "g/h/j/file"), 1000);
    assertTrue(fileSys.exists(new Path(testRootDir, "g/h/j/file")));
    try {
      assertFalse(fileSys.delete(new Path(testRootDir, "g/h"), false));
      fail("Expected delete to fail with recursion turned off");
    } catch (IOException e) {}
    assertTrue(fileSys.exists(new Path(testRootDir, "g/h/j/file")));
    assertTrue(fileSys.delete(new Path(testRootDir, "g/h"), true));
    assertFalse(fileSys.exists(new Path(testRootDir, "g/h/j")));
  }


  @Test(timeout = 3600000)
  public void testOpenCreate() throws IOException {
    if (!liveTest) {
      return;
    }

    try {
      createAndReadFileTest(1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      createAndReadFileTest(5 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      createAndReadFileTest(20 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    /*
    Enable to test the multipart upload
    try {
      createAndReadFileTest((long)6 * 1024 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    */
  }

  @Test(timeout = 1200000)
  public void testRenameFile() throws IOException {
    if (!liveTest) {
      return;
    }

    Path srcPath = new Path(testRootDir, "a/srcfile");

    final OutputStream outputStream = fileSys.create(srcPath, false);
    generateTestData(outputStream, 11 * 1024 * 1024);
    outputStream.close();

    assertTrue(fileSys.exists(srcPath));

    Path dstPath = new Path(testRootDir, "b/dstfile");

    assertFalse(fileSys.rename(srcPath, dstPath));
    assertTrue(fileSys.mkdirs(dstPath.getParent()));
    assertTrue(fileSys.rename(srcPath, dstPath));
    assertTrue(fileSys.exists(dstPath));
    assertFalse(fileSys.exists(srcPath));
    assertTrue(fileSys.exists(srcPath.getParent()));
  }


  @Test(timeout = 10000)
  public void testRenameDirectory() throws IOException {
    if (!liveTest) {
      return;
    }

    Path srcPath = new Path(testRootDir, "a");

    assertTrue(fileSys.mkdirs(srcPath));
    writeFile(new Path(srcPath, "b/testfile"), 1024);

    Path nonEmptyPath = new Path(testRootDir, "nonempty");
    writeFile(new Path(nonEmptyPath, "b/testfile"), 1024);

    assertFalse(fileSys.rename(srcPath, nonEmptyPath));

    Path dstPath = new Path(testRootDir, "b");
    assertTrue(fileSys.rename(srcPath, dstPath));
    assertFalse(fileSys.exists(srcPath));
    assertTrue(fileSys.exists(new Path(dstPath, "b/testfile")));
  }


  @Test(timeout=10000)
  public void testSeek() throws IOException {
    if (!liveTest) {
      return;
    }

    Path path = new Path(testRootDir, "testfile.seek");
    writeFile(path, TEST_BUFFER_SIZE * 10);


    FSDataInputStream inputStream = fileSys.open(path, TEST_BUFFER_SIZE);
    inputStream.seek(inputStream.getPos() + MODULUS);

    testReceivedData(inputStream, TEST_BUFFER_SIZE * 10 - MODULUS);
  }

  /**
   * Creates and reads a file with the given size in S3. The test file is generated according to a specific pattern.
   * During the read phase the incoming data stream is also checked against this pattern.
   *
   * @param fileSize
   * the size of the file to be generated in bytes
   * @throws IOException
   * thrown if an I/O error occurs while writing or reading the test file
   */
  private void createAndReadFileTest(final long fileSize) throws IOException {
    final String objectName = UUID.randomUUID().toString();
    final Path objectPath = new Path(testRootDir, objectName);

    // Write test file to S3
    final OutputStream outputStream = fileSys.create(objectPath, false);
    generateTestData(outputStream, fileSize);
    outputStream.close();

    // Now read the same file back from S3
    final InputStream inputStream = fileSys.open(objectPath);
    testReceivedData(inputStream, fileSize);
    inputStream.close();

    // Delete test file
    fileSys.delete(objectPath, false);
  }


  /**
   * Receives test data from the given input stream and checks the size of the data as well as the pattern inside the
   * received data.
   *
   * @param inputStream
   * the input stream to read the test data from
   * @param expectedSize
   * the expected size of the data to be read from the input stream in bytes
   * @throws IOException
   * thrown if an error occurs while reading the data
   */
  private void testReceivedData(final InputStream inputStream, final long expectedSize) throws IOException {
    final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];

    long totalBytesRead = 0;
    int nextExpectedNumber = 0;
    while (true) {
      final int bytesRead = inputStream.read(testBuffer);
      if (bytesRead < 0) {
        break;
      }

      totalBytesRead += bytesRead;

      for (int i = 0; i < bytesRead; ++i) {
        if (testBuffer[i] != nextExpectedNumber) {
          throw new IOException("Read number " + testBuffer[i] + " but expected " + nextExpectedNumber);
        }

        ++nextExpectedNumber;

        if (nextExpectedNumber == MODULUS) {
          nextExpectedNumber = 0;
        }
      }
    }

    if (totalBytesRead != expectedSize) {
      throw new IOException("Expected to read " + expectedSize + " bytes but only received " + totalBytesRead);
    }
  }


  /**
   * Generates test data of the given size according to some specific pattern and writes it to the provided output
   * stream.
   *
   * @param outputStream
   * the output stream to write the data to
   * @param size
   * the size of the test data to be generated in bytes
   * @throws IOException
   * thrown if an error occurs while writing the data
   */
  private void generateTestData(final OutputStream outputStream, final long size) throws IOException {

    final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];
    for (int i = 0; i < testBuffer.length; ++i) {
      testBuffer[i] = (byte) (i % MODULUS);
    }

    long bytesWritten = 0;
    while (bytesWritten < size) {

      final long diff = size - bytesWritten;
      if (diff < testBuffer.length) {
        outputStream.write(testBuffer, 0, (int)diff);
        bytesWritten += diff;
      } else {
        outputStream.write(testBuffer);
        bytesWritten += testBuffer.length;
      }
    }
  }

  private void writeFile(Path name, int fileSize) throws IOException {
    final OutputStream outputStream = fileSys.create(name, false);
    generateTestData(outputStream, fileSize);
    outputStream.close();
  }
}
