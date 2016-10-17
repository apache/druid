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

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;

import io.druid.common.utils.UUIDUtils;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      Configuration hadoopConfig,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;

    log.info("Configured HDFS as deep storage");
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public String getPathForHadoop()
  {
    return new Path(config.getStorageDirectory()).toUri().toString();
  }

  @Override
  public DataSegment push(File inDir, DataSegment segment) throws IOException
  {
    final String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);

    log.info(
        "Copying segment[%s] to HDFS at location[%s/%s]",
        segment.getIdentifier(),
        config.getStorageDirectory(),
        storageDir
    );

    Path tmpFile = new Path(String.format(
        "%s/%s/index.zip",
        config.getStorageDirectory(),
        UUIDUtils.generateUuid()
    ));
    FileSystem fs = tmpFile.getFileSystem(hadoopConfig);

    fs.mkdirs(tmpFile.getParent());
    log.info("Compressing files from[%s] to [%s]", inDir, tmpFile);

    final long size;
    final DataSegment dataSegment;
    try (FSDataOutputStream out = fs.create(tmpFile)) {
      size = CompressionUtils.zip(inDir, out);
      final Path outFile = new Path(String.format("%s/%s/index.zip", config.getStorageDirectory(), storageDir));
      final Path outDir = outFile.getParent();
      dataSegment = createDescriptorFile(
          segment.withLoadSpec(makeLoadSpec(outFile))
                 .withSize(size)
                 .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir)),
          tmpFile.getParent(),
          fs
      );

      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(outDir.getParent());

      if (!fs.rename(tmpFile.getParent(), outDir)) {
        if (!fs.delete(tmpFile.getParent(), true)) {
          log.error("Failed to delete temp directory[%s]", tmpFile.getParent());
        }
        if (fs.exists(outDir)) {
          log.info(
              "Unable to rename temp directory[%s] to segment directory[%s]. It is already pushed by a replica task.",
              tmpFile.getParent(),
              outDir
          );
        } else {
          throw new IOException(String.format(
              "Failed to rename temp directory[%s] and segment directory[%s] is not present.",
              tmpFile.getParent(),
              outDir
          ));
        }
      }
    }

    return dataSegment;
  }

  private DataSegment createDescriptorFile(DataSegment segment, Path outDir, final FileSystem fs) throws IOException
  {
    final Path descriptorFile = new Path(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    ByteSource
        .wrap(jsonMapper.writeValueAsBytes(segment))
        .copyTo(new HdfsOutputStreamSupplier(fs, descriptorFile));
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(Path outFile)
  {
    return ImmutableMap.<String, Object>of("type", "hdfs", "path", outFile.toUri().toString());
  }

  private static class HdfsOutputStreamSupplier extends ByteSink
  {
    private final FileSystem fs;
    private final Path descriptorFile;

    public HdfsOutputStreamSupplier(FileSystem fs, Path descriptorFile)
    {
      this.fs = fs;
      this.descriptorFile = descriptorFile;
    }

    @Override
    public OutputStream openStream() throws IOException
    {
      return fs.create(descriptorFile);
    }
  }
}
