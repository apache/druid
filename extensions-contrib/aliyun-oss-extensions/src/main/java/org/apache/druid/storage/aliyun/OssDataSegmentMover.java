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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.StorageClass;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;

public class OssDataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(OssDataSegmentMover.class);

  private final OSS client;
  private final OssStorageConfig config;

  @Inject
  public OssDataSegmentMover(
      OSS client,
      OssStorageConfig config
  )
  {
    this.client = client;
    this.config = config;
  }

  @Override
  public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String bucket = MapUtils.getString(loadSpec, "bucket");
      String key = MapUtils.getString(loadSpec, "key");

      final String targetBucket = MapUtils.getString(targetLoadSpec, "bucket");
      final String targetKey = MapUtils.getString(targetLoadSpec, "baseKey");

      final String targetPath = OssUtils.constructSegmentPath(
          targetKey,
          DataSegmentPusher.getDefaultStorageDir(segment, false)
      );

      if (targetBucket.isEmpty()) {
        throw new SegmentLoadingException("Target OSS bucket is not specified");
      }
      if (targetPath.isEmpty()) {
        throw new SegmentLoadingException("Target OSS baseKey is not specified");
      }

      safeMove(bucket, key, targetBucket, targetPath);

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
              .putAll(
                  Maps.filterKeys(
                      loadSpec,
                      new Predicate<String>()
                      {
                        @Override
                        public boolean apply(String input)
                        {
                          return !("bucket".equals(input) || "key".equals(input));
                        }
                      }
                  )
              )
              .put("bucket", targetBucket)
              .put("key", targetPath)
              .build()
      );
    }
    catch (OSSException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]: [%s]", segment.getId(), e);
    }
  }

  private void safeMove(
      final String srcBucket,
      final String srcPath,
      final String targetBucket,
      final String targetPath
  ) throws SegmentLoadingException
  {
    try {
      OssUtils.retry(
          () -> {
            final String copyMsg = StringUtils.format(
                "[%s://%s/%s] to [%s://%s/%s]",
                OssStorageDruidModule.SCHEME,
                srcBucket,
                srcPath,
                OssStorageDruidModule.SCHEME,
                targetBucket,
                targetPath
            );
            try {
              selfCheckingMove(srcBucket, targetBucket, srcPath, targetPath, copyMsg);
              return null;
            }
            catch (OSSException | IOException | SegmentLoadingException e) {
              log.info(e, "Error while trying to move " + copyMsg);
              throw e;
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, OSSException.class);
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      throw new RuntimeException(e);
    }
  }

  /**
   * Copies an object and after that checks that the object is present at the target location, via a separate API call.
   * If it is not, an exception is thrown, and the object is not deleted at the old location. This "paranoic" check
   * is added after it was observed that oss may report a successful move, and the object is not found at the target
   * location.
   */
  private void selfCheckingMove(
      String srcBucket,
      String dstBucket,
      String srcPath,
      String dstPath,
      String copyMsg
  ) throws IOException, SegmentLoadingException
  {
    if (srcBucket.equals(dstBucket) && srcPath.equals(dstPath)) {
      log.info("No need to move file[%s://%s/%s] onto itself", OssStorageDruidModule.SCHEME, srcBucket, srcPath);
      return;
    }
    if (client.doesObjectExist(srcBucket, srcPath)) {
      final ObjectListing listResult = client.listObjects(
          new ListObjectsRequest(srcBucket, srcPath, null, null, 1)
      );
      // Using getObjectSummaries().size() instead of getKeyCount as, in some cases
      // it is observed that even though the getObjectSummaries returns some data
      // keyCount is still zero.
      if (listResult.getObjectSummaries().size() == 0) {
        // should never happen
        throw new ISE("Unable to list object [%s://%s/%s]", OssStorageDruidModule.SCHEME, srcBucket, srcPath);
      }
      final OSSObjectSummary objectSummary = listResult.getObjectSummaries().get(0);
      if (objectSummary.getStorageClass() != null &&
          objectSummary.getStorageClass().equals(StorageClass.IA.name())) {
        throw new OSSException(
            StringUtils.format(
                "Cannot move file[%s://%s/%s] of storage class glacier, skipping.",
                OssStorageDruidModule.SCHEME,
                srcBucket,
                srcPath
            )
        );
      } else {
        log.info("Moving file %s", copyMsg);
        final CopyObjectRequest copyRequest = new CopyObjectRequest(srcBucket, srcPath, dstBucket, dstPath);
        client.copyObject(copyRequest);
        if (!client.doesObjectExist(dstBucket, dstPath)) {
          throw new IOE(
              "After copy was reported as successful the file doesn't exist in the target location [%s]",
              copyMsg
          );
        }
        deleteWithRetriesSilent(srcBucket, srcPath);
        log.debug("Finished moving file %s", copyMsg);
      }
    } else {
      // ensure object exists in target location
      if (client.doesObjectExist(dstBucket, dstPath)) {
        log.info(
            "Not moving file [%s://%s/%s], already present in target location [%s://%s/%s]",
            OssStorageDruidModule.SCHEME,
            srcBucket,
            srcPath,
            OssStorageDruidModule.SCHEME,
            dstBucket,
            dstPath
        );
      } else {
        throw new SegmentLoadingException(
            "Unable to move file %s, not present in either source or target location",
            copyMsg
        );
      }
    }
  }

  private void deleteWithRetriesSilent(final String bucket, final String path)
  {
    try {
      deleteWithRetries(bucket, path);
    }
    catch (Exception e) {
      log.error(e, "Failed to delete file [%s://%s/%s], giving up", OssStorageDruidModule.SCHEME, bucket, path);
    }
  }

  private void deleteWithRetries(final String bucket, final String path) throws Exception
  {
    RetryUtils.retry(
        () -> {
          try {
            client.deleteObject(bucket, path);
            return null;
          }
          catch (Exception e) {
            log.info(e, "Error while trying to delete [%s://%s/%s]", OssStorageDruidModule.SCHEME, bucket, path);
            throw e;
          }
        },
        OssUtils.RETRYABLE,
        3
    );
  }
}
