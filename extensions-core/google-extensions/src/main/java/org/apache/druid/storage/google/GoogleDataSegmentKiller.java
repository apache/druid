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

package org.apache.druid.storage.google;

import com.google.api.client.http.HttpResponseException;
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;

public class GoogleDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger LOG = new Logger(GoogleDataSegmentKiller.class);

  private final GoogleStorage storage;
  private final GoogleAccountConfig accountConfig;
  private final GoogleInputDataConfig inputDataConfig;

  @Inject
  public GoogleDataSegmentKiller(
      final GoogleStorage storage,
      GoogleAccountConfig accountConfig,
      GoogleInputDataConfig inputDataConfig
  )
  {
    this.storage = storage;
    this.accountConfig = accountConfig;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    LOG.info("Killing segment [%s]", segment.getId());

    Map<String, Object> loadSpec = segment.getLoadSpec();
    final String bucket = MapUtils.getString(loadSpec, "bucket");
    final String indexPath = MapUtils.getString(loadSpec, "path");
    final String descriptorPath = DataSegmentKiller.descriptorPath(indexPath);

    try {
      deleteIfPresent(bucket, indexPath);
      // descriptor.json is a file to store segment metadata in deep storage. This file is deprecated and not stored
      // anymore, but we still delete them if exists.
      deleteIfPresent(bucket, descriptorPath);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getId(), e.getMessage());
    }
  }

  private void deleteIfPresent(String bucket, String path) throws IOException
  {
    try {
      RetryUtils.retry(
          (RetryUtils.Task<Void>) () -> {
            storage.delete(bucket, path);
            return null;
          },
          GoogleUtils::isRetryable,
          1,
          5
      );
    }
    catch (HttpResponseException e) {
      if (e.getStatusCode() != 404) {
        throw e;
      }
      LOG.debug("Already deleted: [%s] [%s]", bucket, path);
    }
    catch (IOException ioe) {
      throw ioe;
    }
    catch (Exception e) {
      throw new RE(e, "Failed to delete [%s] [%s]", bucket, path);
    }
  }

  @Override
  public void killAll() throws IOException
  {
    if (accountConfig.getBucket() == null || accountConfig.getPrefix() == null) {
      throw new ISE(
          "Cannot delete all segment files from Google Deep Storage since druid.google.bucket and druid.google.prefix are not both set.");
    }
    LOG.info(
        "Deleting all segment files from gs location [bucket: '%s' prefix: '%s']",
        accountConfig.getBucket(),
        accountConfig.getPrefix()
    );
    try {
      GoogleUtils.deleteObjectsInPath(
          storage,
          inputDataConfig,
          accountConfig.getBucket(),
          accountConfig.getPrefix(),
          Predicates.alwaysTrue()
      );
    }
    catch (Exception e) {
      LOG.error("Error occurred while deleting task log files from gs. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
