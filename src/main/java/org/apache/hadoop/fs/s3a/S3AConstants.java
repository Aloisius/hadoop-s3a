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


public class S3AConstants {
  public static final String ACCESS_KEY = "fs.s3a.awsAccessKeyId";
  public static final String SECRET_KEY = "fs.s3a.awsSecretAccessKey";

  public static final String MAXIMUM_CONNECTIONS = "fs.s3a.maxConnections";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 50;

  public static final String SECURE_CONNECTIONS = "fs.s3a.secureConnections";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;

  public static final String MAX_ERROR_RETRIES = "fs.s3a.maxErrorRetries";
  public static final int DEFAULT_MAX_ERROR_RETRIES = 10;

  public static final String SOCKET_TIMEOUT = "fs.s3a.socketTimeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 50000;

  public static final String MAX_PAGING_KEYS = "fs.s3a.maxPagingKeys";
  public static final int DEFAULT_MAX_PAGING_KEYS = 5000;

  public static final String MULTIPART_SIZE = "fs.s3a.multipartSize";
  public static final long DEFAULT_MULTIPART_SIZE = (long)5 * 1024 * 1024;

  public static final String MIN_MULTIPART_THRESHOLD = "fs.s3a.minMultipartSize";
  public static final int DEFAULT_MIN_MULTIPART_THRESHOLD = 5 * 1024 * 1024;

  public static final String BUFFER_DIR = "fs.s3a.buffer.dir";

  // private | public-read | public-read-write | authenticated-read | log-delivery-write | bucket-owner-read | bucket-owner-full-control
  public static final String CANNED_ACL = "fs.s3a.cannedACL";
  public static final String DEFAULT_CANNED_ACL = "";

  public static final String PURGE_EXISTING_MULTIPART = "fs.s3a.purgeExistingMultiPart";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  public static final String PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.purgeExistingMultiPartAge";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;
}
