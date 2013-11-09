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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;

import org.apache.commons.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;


public class S3ACopyTransferManager {
  private ThreadPoolExecutor threadPool;
  private AmazonS3 s3;
  public static final Log LOG = S3AFileSystem.LOG;
  private long partSize;

  S3ACopyTransferManager(AmazonS3 s3, long partSize) {
    ThreadFactory threadFactory = new ThreadFactory() {
      private int threadCount = 1;

      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("s3a-transfer-manager-worker-" + threadCount++);
        return thread;
      }
    };
    this.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10, threadFactory);
    this.s3 = s3;
    this.partSize = partSize;
  }

  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
      throws AmazonClientException, AmazonServiceException, InterruptedException {
    ObjectMetadata srcMeta = s3.getObjectMetadata(copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey());

    long objectSize = srcMeta.getContentLength();
    double optimalPartSize = (double)objectSize / (double)com.amazonaws.services.s3.internal.Constants.MAXIMUM_UPLOAD_PARTS;
    optimalPartSize = Math.ceil(optimalPartSize);
    long copyPartSize = (long)Math.max(optimalPartSize, partSize);

    InitiateMultipartUploadRequest initiateRequest =
        new InitiateMultipartUploadRequest(copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());

    InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initiateRequest);

    List<CopyPartCallable> tasks = new ArrayList<CopyPartCallable>();

    long bytePosition = 0;
    for (int i = 1; bytePosition < objectSize; i++) {
      CopyPartRequest copyRequest = new CopyPartRequest()
          .withDestinationBucketName(copyObjectRequest.getDestinationBucketName())
          .withDestinationKey(copyObjectRequest.getDestinationKey())
          .withSourceBucketName(copyObjectRequest.getSourceBucketName())
          .withSourceKey(copyObjectRequest.getSourceKey())
          .withUploadId(initResult.getUploadId())
          .withFirstByte(bytePosition)
          .withLastByte(bytePosition + copyPartSize - 1 >= objectSize ? objectSize - 1 : bytePosition + copyPartSize - 1)
          .withPartNumber(i);

      tasks.add(new CopyPartCallable(s3, copyRequest));
      bytePosition += copyPartSize;
    }


    final List<Future<PartETag>> futures = threadPool.invokeAll(tasks);
    final List<PartETag> partETags = new ArrayList<PartETag>(tasks.size());
    for (Future<PartETag> future : futures) {
      if (future.isCancelled()) {
        throw new CancellationException();
      }

      try {
        partETags.add(future.get());
      } catch (ExecutionException e) {
          Throwable t = e.getCause();
          if (t instanceof AmazonClientException) throw (AmazonClientException)t;
          throw new AmazonClientException("Unable to complete copy: " + t.getMessage(), t);
      } catch (Exception e) {
        throw new AmazonClientException("Unable to copy part: " + e.getCause().getMessage(), e.getCause());
      }
    }

    CompleteMultipartUploadRequest completeRequest = new
        CompleteMultipartUploadRequest(copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey(),
        initResult.getUploadId(), partETags);

    CompleteMultipartUploadResult completeUploadResponse = s3.completeMultipartUpload(completeRequest);
    CopyObjectResult result = new CopyObjectResult();
    result.setETag(completeUploadResponse.getETag());
    result.setExpirationTime(completeUploadResponse.getExpirationTime());
    result.setExpirationTimeRuleId(completeUploadResponse.getExpirationTimeRuleId());
    result.setLastModifiedDate(srcMeta.getLastModified());
    result.setServerSideEncryption(completeUploadResponse.getServerSideEncryption());
    result.setVersionId(completeUploadResponse.getVersionId());
    return result;
  }

  public class CopyPartCallable implements Callable<PartETag> {
    private final AmazonS3 s3;
    private final CopyPartRequest request;

    public CopyPartCallable(AmazonS3 s3, CopyPartRequest request) {
      this.s3 = s3;
      this.request = request;
    }

    public PartETag call() throws Exception {
      return s3.copyPart(request).getPartETag();
    }
  }
}
