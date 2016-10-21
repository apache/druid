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

import java.io.File;
import java.io.IOException;

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
    File outDir = new File(config.getStorageDirectory(), DataSegmentPusherUtil.getStorageDir(segment));

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

    if (!outDir.mkdirs() && !outDir.isDirectory()) {
      throw new IOException(String.format("Cannot create directory[%s]", outDir));
    }
    File outFile = new File(outDir, "index.zip");
    log.info("Compressing files from[%s] to [%s]", dataSegmentFile, outFile);
    long size = CompressionUtils.zip(dataSegmentFile, outFile);

    return createDescriptorFile(
        segment.withLoadSpec(makeLoadSpec(outFile))
               .withSize(size)
               .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
        outDir
    );
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
