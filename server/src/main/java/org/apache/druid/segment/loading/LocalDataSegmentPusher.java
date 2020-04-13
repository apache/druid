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

package org.apache.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  private static final String INDEX_FILENAME = "index.zip";

  private final LocalDataSegmentPusherConfig config;

  @Inject
  public LocalDataSegmentPusher(LocalDataSegmentPusherConfig config)
  {
    this.config = config;
  }

  @Override
  public String getPathForHadoop()
  {
    return config.getStorageDirectory().getAbsoluteFile().toURI().toString();
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(final File dataSegmentFile, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    final File baseStorageDir = config.getStorageDirectory();
    final File outDir = new File(baseStorageDir, this.getStorageDir(segment, useUniquePath));

    log.debug("Copying segment[%s] to local filesystem at location[%s]", segment.getId(), outDir.toString());

    if (dataSegmentFile.equals(outDir)) {
      long size = 0;
      for (File file : dataSegmentFile.listFiles()) {
        size += file.length();
      }

      return segment.withLoadSpec(makeLoadSpec(outDir.toURI()))
                    .withSize(size)
                    .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile));
    }

    final File tmpOutDir = new File(baseStorageDir, makeIntermediateDir());
    log.debug("Creating intermediate directory[%s] for segment[%s].", tmpOutDir.toString(), segment.getId());
    org.apache.commons.io.FileUtils.forceMkdir(tmpOutDir);

    try {
      final File tmpIndexFile = new File(tmpOutDir, INDEX_FILENAME);
      final long size = compressSegment(dataSegmentFile, tmpIndexFile);

      final DataSegment dataSegment = segment.withLoadSpec(makeLoadSpec(new File(outDir, INDEX_FILENAME).toURI()))
                                       .withSize(size)
                                       .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile));

      org.apache.commons.io.FileUtils.forceMkdir(outDir);
      final File indexFileTarget = new File(outDir, tmpIndexFile.getName());

      if (!tmpIndexFile.renameTo(indexFileTarget)) {
        throw new IOE("Failed to rename [%s] to [%s]", tmpIndexFile, indexFileTarget);
      }

      return dataSegment;
    }
    finally {
      FileUtils.deleteDirectory(tmpOutDir);
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    return ImmutableMap.of("type", "local", "path", finalIndexZipFilePath.getPath());
  }

  private String makeIntermediateDir()
  {
    return "intermediate_pushes/" + UUID.randomUUID();
  }

  private long compressSegment(File dataSegmentFile, File dest) throws IOException
  {
    log.debug("Compressing files from[%s] to [%s]", dataSegmentFile, dest);
    return CompressionUtils.zip(dataSegmentFile, dest, true);
  }
}
