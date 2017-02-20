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


public class Constants {
  // s3 access key
  public static final String OLD_ACCESS_KEY = "fs.s3a.awsAccessKeyId";
  public static final String NEW_ACCESS_KEY = "fs.s3a.access.key";

  // s3 secret key
  public static final String OLD_SECRET_KEY = "fs.s3a.awsSecretAccessKey";
  public static final String NEW_SECRET_KEY = "fs.s3a.secret.key";
  
  // number of simultaneous connections to s3
  public static final String OLD_MAXIMUM_CONNECTIONS = "fs.s3a.maxConnections";
  public static final String NEW_MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 15;
  
  // connect to s3 over ssl?
  public static final String OLD_SECURE_CONNECTIONS = "fs.s3a.secureConnections";
  public static final String NEW_SECURE_CONNECTIONS = "fs.s3a.connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;
  
  // number of times we should retry errors
  public static final String OLD_MAX_ERROR_RETRIES = "fs.s3a.maxErrorRetries";
  public static final String NEW_MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";
  public static final int DEFAULT_MAX_ERROR_RETRIES = 10;
  
  // seconds until we give up on a connection to s3
  public static final String OLD_SOCKET_TIMEOUT = "fs.s3a.socketTimeout";
  public static final String NEW_SOCKET_TIMEOUT = "fs.s3a.connection.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 50000;

  // number of records to get while paging through a directory listing
  public static final String OLD_MAX_PAGING_KEYS = "fs.s3a.maxPagingKeys";
  public static final String NEW_MAX_PAGING_KEYS = "fs.s3a.paging.maximum";
  public static final int DEFAULT_MAX_PAGING_KEYS = 5000;

  // size of each of or multipart pieces in bytes
  public static final String OLD_MULTIPART_SIZE = "fs.s3a.multipartSize";
  public static final String NEW_MULTIPART_SIZE = "fs.s3a.multipart.size";
  public static final long DEFAULT_MULTIPART_SIZE = 104857600; // 100 MB
  
  // minimum size in bytes before we start a multipart uploads or copy
  public static final String OLD_MIN_MULTIPART_THRESHOLD = "fs.s3a.minMultipartSize";
  public static final String NEW_MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold";
  public static final int DEFAULT_MIN_MULTIPART_THRESHOLD = Integer.MAX_VALUE;
  
  // comma separated list of directories
  public static final String BUFFER_DIR = "fs.s3a.buffer.dir";

  // private | public-read | public-read-write | authenticated-read | log-delivery-write | bucket-owner-read | bucket-owner-full-control
  public static final String OLD_CANNED_ACL = "fs.s3a.cannedACL";
  public static final String NEW_CANNED_ACL = "fs.s3a.acl.default";
  public static final String DEFAULT_CANNED_ACL = "";

  // should we try to purge old multipart uploads when starting up
  public static final String OLD_PURGE_EXISTING_MULTIPART = "fs.s3a.purgeExistingMultiPart";
  public static final String NEW_PURGE_EXISTING_MULTIPART = "fs.s3a.multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  // purge any multipart uploads older than this number of seconds
  public static final String OLD_PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.purgeExistingMultiPartAge";
  public static final String NEW_PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 14400;
  
  //s3 server-side encryption
  public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.s3a.server-side-encryption-algorithm";
  
  public static final String S3N_FOLDER_SUFFIX = "_$folder$";

	//s3 region configuration
	public static final String S3_REGION = "fs.s3a.region";
}
