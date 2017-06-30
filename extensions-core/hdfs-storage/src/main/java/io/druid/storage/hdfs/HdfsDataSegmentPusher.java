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
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopFsWrapper;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

/**
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;
  private final String fullyQualifiedStorageDirectory;

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      Configuration hadoopConfig,
      ObjectMapper jsonMapper
  ) throws IOException
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;
    Path storageDir = new Path(config.getStorageDirectory());
    this.fullyQualifiedStorageDirectory = FileSystem.newInstance(storageDir.toUri(), hadoopConfig)
                                                    .makeQualified(storageDir)
                                                    .toUri()
                                                    .toString();

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
    return fullyQualifiedStorageDirectory;
  }

  @Override
  public DataSegment push(File inDir, DataSegment segment) throws IOException
  {
    final String storageDir = this.getStorageDir(segment);

    log.info(
        "Copying segment[%s] to HDFS at location[%s/%s]",
        segment.getIdentifier(),
        fullyQualifiedStorageDirectory,
        storageDir
    );

    Path tmpIndexFile = new Path(StringUtils.format(
        "%s/%s/%s/%s_index.zip",
        fullyQualifiedStorageDirectory,
        segment.getDataSource(),
        UUIDUtils.generateUuid(),
        segment.getShardSpec().getPartitionNum()
    ));
    FileSystem fs = tmpIndexFile.getFileSystem(hadoopConfig);

    fs.mkdirs(tmpIndexFile.getParent());
    log.info("Compressing files from[%s] to [%s]", inDir, tmpIndexFile);

    final long size;
    final DataSegment dataSegment;
    try (FSDataOutputStream out = fs.create(tmpIndexFile)) {
      size = CompressionUtils.zip(inDir, out);
      final Path outIndexFile = new Path(StringUtils.format(
          "%s/%s/%d_index.zip",
          fullyQualifiedStorageDirectory,
          storageDir,
          segment.getShardSpec().getPartitionNum()
      ));
      final Path outDescriptorFile = new Path(StringUtils.format(
          "%s/%s/%d_descriptor.json",
          fullyQualifiedStorageDirectory,
          storageDir,
          segment.getShardSpec().getPartitionNum()
      ));

      dataSegment = segment.withLoadSpec(makeLoadSpec(outIndexFile.toUri()))
                           .withSize(size)
                           .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir));

      final Path tmpDescriptorFile = new Path(
          tmpIndexFile.getParent(),
          StringUtils.format("%s_descriptor.json", dataSegment.getShardSpec().getPartitionNum())
      );

      log.info("Creating descriptor file at[%s]", tmpDescriptorFile);
      ByteSource
          .wrap(jsonMapper.writeValueAsBytes(dataSegment))
          .copyTo(new HdfsOutputStreamSupplier(fs, tmpDescriptorFile));

      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(outIndexFile.getParent());
      copyFilesWithChecks(fs, tmpDescriptorFile, outDescriptorFile);
      copyFilesWithChecks(fs, tmpIndexFile, outIndexFile);
    }
    finally {
      try {
        if (fs.exists(tmpIndexFile.getParent()) && !fs.delete(tmpIndexFile.getParent(), true)) {
          log.error("Failed to delete temp directory[%s]", tmpIndexFile.getParent());
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to delete temp directory[%s]", tmpIndexFile.getParent());
      }
    }

    return dataSegment;
  }

  private void copyFilesWithChecks(final FileSystem fs, final Path from, final Path to) throws IOException
  {
    if (!HadoopFsWrapper.rename(fs, from, to)) {
      if (fs.exists(to)) {
        log.info(
            "Unable to rename temp Index file[%s] to final segment path [%s]. "
            + "It is already pushed by a replica task.",
            from,
            to
        );
      } else {
        throw new IOE("Failed to rename temp Index file[%s] and final segment path[%s] is not present.", from, to);
      }
    }
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

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    return  ImmutableMap.<String, Object>of("type", "hdfs", "path", finalIndexZipFilePath.toString());
  }

  /**
   * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
   * path names. So we format paths differently for HDFS.
   */

  @Override
  public String getStorageDir(DataSegment segment)
  {
    return JOINER.join(
        segment.getDataSource(),
        StringUtils.format(
            "%s_%s",
            segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
            segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
        ),
        segment.getVersion().replaceAll(":", "_")
    );
  }

  @Override
  public String makeIndexPathName(DataSegment dataSegment, String indexName)
  {
    return StringUtils.format(
        "./%s/%d_%s",
        this.getStorageDir(dataSegment),
        dataSegment.getShardSpec().getPartitionNum(),
        indexName
    );
  }
}
