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
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;

public class OssDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(OssDataSegmentKiller.class);

  private final OSS client;
  private final OssStorageConfig segmentPusherConfig;
  private final OssInputDataConfig inputDataConfig;

  @Inject
  public OssDataSegmentKiller(
      OSS client,
      OssStorageConfig segmentPusherConfig,
      OssInputDataConfig inputDataConfig
  )
  {
    this.client = client;
    this.segmentPusherConfig = segmentPusherConfig;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String bucket = MapUtils.getString(loadSpec, "bucket");
      String path = MapUtils.getString(loadSpec, "key");

      if (client.doesObjectExist(bucket, path)) {
        log.info("Removing index file[%s://%s/%s] from aliyun OSS!", OssStorageDruidModule.SCHEME, bucket, path);
        client.deleteObject(bucket, path);
      }
    }
    catch (OSSException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getId(), e);
    }
  }

  @Override
  public void killAll() throws IOException
  {
    if (segmentPusherConfig.getBucket() == null || segmentPusherConfig.getPrefix() == null) {
      throw new ISE(
          "Cannot delete all segment from aliyun OSS Deep Storage since druid.storage.bucket and druid.storage.baseKey are not both set.");
    }
    log.info("Deleting all segment files from aliyun OSS location [bucket: '%s' prefix: '%s']",
             segmentPusherConfig.getBucket(), segmentPusherConfig.getPrefix()
    );
    try {
      OssUtils.deleteObjectsInPath(
          client,
          inputDataConfig,
          segmentPusherConfig.getBucket(),
          segmentPusherConfig.getPrefix(),
          Predicates.alwaysTrue()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting segment files from aliyun OSS. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
