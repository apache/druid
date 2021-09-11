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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;

import java.io.File;
import java.io.IOException;

public class DeepStorageShuffleClient implements ShuffleClient<DeepStoragePartitionLocation>
{
  private static final Logger LOG = new Logger(DeepStorageShuffleClient.class);
  private final ObjectMapper objectMapper;

  @Inject
  public DeepStorageShuffleClient(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  @Override
  public File fetchSegmentFile(File partitionDir, String supervisorTaskId, DeepStoragePartitionLocation location)
      throws IOException
  {
    final LoadSpec loadSpec = objectMapper.convertValue(location.getLoadSpec(), LoadSpec.class);
    final File unzippedDir = new File(partitionDir, StringUtils.format("unzipped_%s", location.getSubTaskId()));
    FileUtils.forceMkdir(unzippedDir);
    try {
      loadSpec.loadSegment(unzippedDir);
    }
    catch (SegmentLoadingException e) {
      LOG.error(e, "Failed to load segment");
      throw new IOException(e);
    }
    return unzippedDir;
  }
}
