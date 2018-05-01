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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.UUID;

public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  private static final String INDEX_FILENAME = "index.zip";
  private static final String DESCRIPTOR_FILENAME = "descriptor.json";

  private final LocalDataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public LocalDataSegmentPusher(LocalDataSegmentPusherConfig config, ObjectMapper jsonMapper)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;

    log.info("Configured local filesystem as deep storage");
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

    log.info("Copying segment[%s] to local filesystem at location[%s]", segment.getIdentifier(), outDir.toString());

    if (dataSegmentFile.equals(outDir)) {
      long size = 0;
      for (File file : dataSegmentFile.listFiles()) {
        size += file.length();
      }

      return createDescriptorFile(
          segment.withLoadSpec(makeLoadSpec(outDir.toURI()))
                 .withSize(size)
                 .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
          outDir
      );
    }

    final File tmpOutDir = new File(baseStorageDir, makeIntermediateDir());
    log.info("Creating intermediate directory[%s] for segment[%s]", tmpOutDir.toString(), segment.getIdentifier());
    FileUtils.forceMkdir(tmpOutDir);

    try {
      final File tmpIndexFile = new File(tmpOutDir, INDEX_FILENAME);
      final long size = compressSegment(dataSegmentFile, tmpIndexFile);

      final File tmpDescriptorFile = new File(tmpOutDir, DESCRIPTOR_FILENAME);
      DataSegment dataSegment = createDescriptorFile(
          segment.withLoadSpec(makeLoadSpec(new File(outDir, INDEX_FILENAME).toURI()))
                 .withSize(size)
                 .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
          tmpDescriptorFile
      );

      FileUtils.forceMkdir(outDir);
      final File indexFileTarget = new File(outDir, tmpIndexFile.getName());
      final File descriptorFileTarget = new File(outDir, tmpDescriptorFile.getName());

      if (!tmpIndexFile.renameTo(indexFileTarget)) {
        throw new IOE("Failed to rename [%s] to [%s]", tmpIndexFile, indexFileTarget);
      }

      if (!tmpDescriptorFile.renameTo(descriptorFileTarget)) {
        throw new IOE("Failed to rename [%s] to [%s]", tmpDescriptorFile, descriptorFileTarget);
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
    return ImmutableMap.<String, Object>of("type", "local", "path", finalIndexZipFilePath.getPath());
  }

  private String makeIntermediateDir()
  {
    return "intermediate_pushes/" + UUID.randomUUID().toString();
  }

  private long compressSegment(File dataSegmentFile, File dest) throws IOException
  {
    log.info("Compressing files from[%s] to [%s]", dataSegmentFile, dest);
    return CompressionUtils.zip(dataSegmentFile, dest, true);
  }

  private DataSegment createDescriptorFile(DataSegment segment, File dest) throws IOException
  {
    log.info("Creating descriptor file at[%s]", dest);
    // Avoid using Guava in DataSegmentPushers because they might be used with very diverse Guava versions in
    // runtime, and because Guava deletes methods over time, that causes incompatibilities.
    Files.write(
        dest.toPath(), jsonMapper.writeValueAsBytes(segment), StandardOpenOption.CREATE, StandardOpenOption.SYNC
    );

    return segment;
  }
}
