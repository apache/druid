/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.google;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.inject.Inject;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;

public class GoogleDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger LOG = new Logger(GoogleDataSegmentKiller.class);

  private final GoogleStorage storage;

  @Inject
  public GoogleDataSegmentKiller(final GoogleStorage storage)
  {
    this.storage = storage;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    LOG.info("Killing segment [%s]", segment);

    Map<String, Object> loadSpec = segment.getLoadSpec();
    final String bucket = MapUtils.getString(loadSpec, "bucket");
    final String indexPath = MapUtils.getString(loadSpec, "path");
    final String descriptorPath = indexPath.substring(0, indexPath.lastIndexOf("/")) + "/descriptor.json";

    try {
      deleteIfPresent(bucket, indexPath);
      deleteIfPresent(bucket, descriptorPath);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getIdentifier(), e.getMessage());
    }
  }

  private void deleteIfPresent(String bucket, String path) throws IOException
  {
    try {
      storage.delete(bucket, path);
    }
    catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != 404) {
        throw e;
      }
      LOG.debug("Already deleted: [%s] [%s]", bucket, path);
    }
  }

  @Override
  public void killAll()
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
