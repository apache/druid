/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class S3DataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final S3DataSegmentPusherConfig segmentPusherConfig;
  private final S3InputDataConfig inputDataConfig;

  @Inject
  public S3DataSegmentKiller(
      ServerSideEncryptingAmazonS3 s3Client,
      S3DataSegmentPusherConfig segmentPusherConfig,
      S3InputDataConfig inputDataConfig
  )
  {
    this.s3Client = s3Client;
    this.segmentPusherConfig = segmentPusherConfig;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = DataSegmentKiller.descriptorPath(s3Path);

      if (s3Client.doesObjectExist(s3Bucket, s3Path)) {
        log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
        s3Client.deleteObject(s3Bucket, s3Path);
      }
      // descriptor.json is a file to store segment metadata in deep storage. This file is deprecated and not stored
      // anymore, but we still delete them if exists.
      if (s3Client.doesObjectExist(s3Bucket, s3DescriptorPath)) {
        log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
        s3Client.deleteObject(s3Bucket, s3DescriptorPath);
      }
    }
    catch (AmazonServiceException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getId(), e);
    }
  }

  @Override
  public void killAll() throws IOException
  {
    try {
      S3Utils.retryS3Operation(
          () -> {
            String bucketName = segmentPusherConfig.getBucket();
            String prefix = segmentPusherConfig.getBaseKey();
            int maxListingLength = inputDataConfig.getMaxListingLength();
            ListObjectsV2Result result;
            String continuationToken = null;
            do {
              log.info("Deleting batch of %d segment files from s3 location [bucket: %s    prefix: %s].",
                       maxListingLength, bucketName, prefix
              );
              ListObjectsV2Request request = new ListObjectsV2Request()
                  .withBucketName(bucketName)
                  .withPrefix(prefix)
                  .withContinuationToken(continuationToken)
                  .withMaxKeys(maxListingLength);

              result = s3Client.listObjectsV2(request);
              List<S3ObjectSummary> objectSummaries = result.getObjectSummaries();

              List<DeleteObjectsRequest.KeyVersion> keyVersionsToDelete =
                  objectSummaries.stream()
                                 .map(x -> new DeleteObjectsRequest.KeyVersion(x.getKey()))
                                 .collect(Collectors.toList());

              DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName)
                  .withBucketName(bucketName)
                  .withKeys(keyVersionsToDelete);
              s3Client.deleteObjects(deleteRequest);

              continuationToken = result.getNextContinuationToken();
            } while (result.isTruncated());
            return null;
          }
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting segment files from s3. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
