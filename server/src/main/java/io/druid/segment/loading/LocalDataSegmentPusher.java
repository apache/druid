/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

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
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return String.format("file://%s/%s", config.getStorageDirectory(), dataSource);
  }

  @Override
  public DataSegment push(File dataSegmentFile, DataSegment segment) throws IOException
  {
    File outDir = new File(config.getStorageDirectory(), DataSegmentPusherUtil.getStorageDir(segment));

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

    outDir.mkdirs();
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
