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
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class S3DataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);

  // AWS has max limit of 1000 objects that can be requested to be deleted at a time.
  private static final int MAX_MULTI_OBJECT_DELETE_SIZE = 1000;

  /**
   * Any implementation of DataSegmentKiller is initialized when an ingestion job starts if the extension is loaded,
   * even when the implementation of DataSegmentKiller is not used. As a result, if we have a s3 client instead
   * of a supplier of it, it can cause unnecessary config validation for s3 even when it's not used at all.
   * To perform the config validation only when it is actually used, we use a supplier.
   * <p>
   * See OmniDataSegmentKiller for how DataSegmentKillers are initialized.
   */
  private final Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier;
  private final S3DataSegmentPusherConfig segmentPusherConfig;
  private final S3InputDataConfig inputDataConfig;

  @Inject
  public S3DataSegmentKiller(
      Supplier<ServerSideEncryptingAmazonS3> s3Client,
      S3DataSegmentPusherConfig segmentPusherConfig,
      S3InputDataConfig inputDataConfig
  )
  {
    this.s3ClientSupplier = s3Client;
    this.segmentPusherConfig = segmentPusherConfig;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  public void kill(List<DataSegment> segments) throws SegmentLoadingException
  {
    if (segments.isEmpty()) {
      return;
    }
    if (segments.size() == 1) {
      kill(segments.get(0));
      return;
    }

    // create a map of bucket to keys to delete
    Map<String, List<DeleteObjectsRequest.KeyVersion>> bucketToKeysToDelete = new HashMap<>();
    for (DataSegment segment : segments) {
      String s3Bucket = MapUtils.getString(segment.getLoadSpec(), S3DataSegmentPuller.BUCKET);
      String path = MapUtils.getString(segment.getLoadSpec(), S3DataSegmentPuller.KEY);
      List<DeleteObjectsRequest.KeyVersion> keysToDelete = bucketToKeysToDelete.computeIfAbsent(
          s3Bucket,
          k -> new ArrayList<>()
      );
      keysToDelete.add(new DeleteObjectsRequest.KeyVersion(path));
      keysToDelete.add(new DeleteObjectsRequest.KeyVersion(DataSegmentKiller.descriptorPath(path)));
    }

    final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();
    boolean shouldThrowException = false;
    for (Map.Entry<String, List<DeleteObjectsRequest.KeyVersion>> bucketToKeys : bucketToKeysToDelete.entrySet()) {
      String s3Bucket = bucketToKeys.getKey();
      List<DeleteObjectsRequest.KeyVersion> keysToDelete = bucketToKeys.getValue();
      boolean hadException = deleteKeysForBucket(s3Client, s3Bucket, keysToDelete);
      if (hadException) {
        shouldThrowException = true;
      }
    }
    if (shouldThrowException) {
      // exception error message gets cutoff without providing any details. look at the logs for more details.
      // this was a shortcut to handle the many different ways there could potentially be failures and handle them
      // reasonably
      throw new SegmentLoadingException(
          "Couldn't delete segments from S3. See the task logs for more details."
      );
    }
  }

  /**
   * Delete all keys in a bucket from s3
   *
   * @param s3Client     client used to communicate with s3
   * @param s3Bucket     the bucket where the keys exist
   * @param keysToDelete the keys to delete
   * @return a boolean value of true if there was an issue deleting one or many keys, a boolean value of false if
   * succesful
   */
  private boolean deleteKeysForBucket(
      ServerSideEncryptingAmazonS3 s3Client,
      String s3Bucket,
      List<DeleteObjectsRequest.KeyVersion> keysToDelete
  )
  {
    boolean hadException = false;
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(s3Bucket);
    deleteObjectsRequest.setQuiet(true);
    List<List<DeleteObjectsRequest.KeyVersion>> keysChunks = Lists.partition(
        keysToDelete,
        MAX_MULTI_OBJECT_DELETE_SIZE
    );
    for (List<DeleteObjectsRequest.KeyVersion> chunkOfKeys : keysChunks) {
      List<String> keysToDeleteStrings = chunkOfKeys.stream().map(
          DeleteObjectsRequest.KeyVersion::getKey).collect(Collectors.toList());
      try {
        deleteObjectsRequest.setKeys(chunkOfKeys);
        log.info(
            "Removing from bucket: [%s] the following index files: [%s] from s3!",
            s3Bucket,
            keysToDeleteStrings
        );
        s3Client.deleteObjects(deleteObjectsRequest);
      }
      catch (MultiObjectDeleteException e) {
        hadException = true;
        Map<String, List<String>> errorToKeys = new HashMap<>();
        for (MultiObjectDeleteException.DeleteError error : e.getErrors()) {
          errorToKeys.computeIfAbsent(error.getMessage(), k -> new ArrayList<>()).add(error.getKey());
        }
        errorToKeys.forEach((key, value) -> log.error(
            "Unable to delete from bucket [%s], the following keys [%s], because [%s]",
            s3Bucket,
            String.join(", ", value),
            key
        ));
      }
      catch (AmazonServiceException e) {
        hadException = true;
        log.noStackTrace().warn(e,
            "Unable to delete from bucket [%s], the following keys [%s]",
            s3Bucket,
            chunkOfKeys.stream().map(DeleteObjectsRequest.KeyVersion::getKey).collect(Collectors.joining(", "))
        );
      }
    }
    return hadException;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, S3DataSegmentPuller.BUCKET);
      String s3Path = MapUtils.getString(loadSpec, S3DataSegmentPuller.KEY);
      String s3DescriptorPath = DataSegmentKiller.descriptorPath(s3Path);

      final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();
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
    if (segmentPusherConfig.getBucket() == null || segmentPusherConfig.getBaseKey() == null) {
      throw new ISE(
          "Cannot delete all segment from S3 Deep Storage since druid.storage.bucket and druid.storage.baseKey are not both set.");
    }
    log.info("Deleting all segment files from s3 location [bucket: '%s' prefix: '%s']",
             segmentPusherConfig.getBucket(), segmentPusherConfig.getBaseKey()
    );
    try {
      S3Utils.deleteObjectsInPath(
          s3ClientSupplier.get(),
          inputDataConfig.getMaxListingLength(),
          segmentPusherConfig.getBucket(),
          segmentPusherConfig.getBaseKey(),
          Predicates.alwaysTrue()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting segment files from s3. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
