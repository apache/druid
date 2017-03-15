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
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.UUID;

/**
 */
public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  private final LocalDataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public LocalDataSegmentPusher(
      LocalDataSegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
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
  public DataSegment push(File dataSegmentFile, DataSegment segment) throws IOException
  {
    final String storageDir = DataSegmentPusherUtil.getStorageDir(segment);
    final File baseStorageDir = config.getStorageDirectory();
    final File outDir = new File(baseStorageDir, storageDir);

    log.info("Copying segment[%s] to local filesystem at location[%s]", segment.getIdentifier(), outDir.toString());

    if (dataSegmentFile.equals(outDir)) {
      long size = 0;
      for (File file : dataSegmentFile.listFiles()) {
        size += file.length();
      }

      return createDescriptorFile(
          segment.withLoadSpec(makeLoadSpec(outDir))
                 .withSize(size)
                 .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
          outDir
      );
    }

    final File tmpOutDir = new File(baseStorageDir, intermediateDirFor(storageDir));
    log.info("Creating intermediate directory[%s] for segment[%s]", tmpOutDir.toString(), segment.getIdentifier());
    final long size = compressSegment(dataSegmentFile, tmpOutDir);

    final DataSegment dataSegment = createDescriptorFile(
      segment.withLoadSpec(makeLoadSpec(new File(outDir, "index.zip")))
             .withSize(size)
             .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
      tmpOutDir
    );

    // moving the temporary directory to the final destination, once success the potentially concurrent push operations
    // will be failed and will read the descriptor.json created by current push operation directly
    FileUtils.forceMkdir(outDir.getParentFile());
    try {
      java.nio.file.Files.move(tmpOutDir.toPath(), outDir.toPath());
    }
    catch (FileAlreadyExistsException e) {
      log.warn("Push destination directory[%s] exists, ignore this message if replication is configured.", outDir);
      FileUtils.deleteDirectory(tmpOutDir);
      return jsonMapper.readValue(new File(outDir, "descriptor.json"), DataSegment.class);
    }
    return dataSegment;
  }

  private String intermediateDirFor(String storageDir)
  {
    return "intermediate_pushes/" + storageDir + "." + UUID.randomUUID().toString();
  }

  private long compressSegment(File dataSegmentFile, File outDir) throws IOException
  {
    FileUtils.forceMkdir(outDir);
    File outFile = new File(outDir, "index.zip");
    log.info("Compressing files from[%s] to [%s]", dataSegmentFile, outFile);
    return CompressionUtils.zip(dataSegmentFile, outFile);
  }

  private DataSegment createDescriptorFile(DataSegment segment, File outDir) throws IOException
  {
    File descriptorFile = new File(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    Files.copy(ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(segment)), descriptorFile);
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(File outFile)
  {
    return ImmutableMap.<String, Object>of("type", "local", "path", outFile.toString());
  }
}
