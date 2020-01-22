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

package org.apache.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopFsWrapper;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 *
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;

  // We lazily initialize fullQualifiedStorageDirectory to avoid potential issues with Hadoop namenode HA.
  // Please see https://github.com/apache/druid/pull/5684
  private final Supplier<String> fullyQualifiedStorageDirectory;

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      @Hdfs Configuration hadoopConfig,
      ObjectMapper jsonMapper
  )
  {
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;
    Path storageDir = new Path(config.getStorageDirectory());
    this.fullyQualifiedStorageDirectory = Suppliers.memoize(
        () -> {
          try {
            return FileSystem.newInstance(storageDir.toUri(), hadoopConfig)
                             .makeQualified(storageDir)
                             .toUri()
                             .toString();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    );
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
    return fullyQualifiedStorageDirectory.get();
  }

  @Override
  public DataSegment push(final File inDir, final DataSegment segment, final boolean useUniquePath) throws IOException
  {
    // For HDFS, useUniquePath does not affect the directory tree but instead affects the filename, which is of the form
    // '{partitionNum}_index.zip' without unique paths and '{partitionNum}_{UUID}_index.zip' with unique paths.
    final String storageDir = this.getStorageDir(segment, false);

    log.debug(
        "Copying segment[%s] to HDFS at location[%s/%s]",
        segment.getId(),
        fullyQualifiedStorageDirectory.get(),
        storageDir
    );

    Path tmpIndexFile = new Path(StringUtils.format(
        "%s/%s/%s/%s_index.zip",
        fullyQualifiedStorageDirectory.get(),
        segment.getDataSource(),
        UUIDUtils.generateUuid(),
        segment.getShardSpec().getPartitionNum()
    ));
    FileSystem fs = tmpIndexFile.getFileSystem(hadoopConfig);

    fs.mkdirs(tmpIndexFile.getParent());
    log.debug("Compressing files from[%s] to [%s]", inDir, tmpIndexFile);

    final long size;
    final DataSegment dataSegment;
    try {
      try (FSDataOutputStream out = fs.create(tmpIndexFile)) {
        size = CompressionUtils.zip(inDir, out);
      }

      final String uniquePrefix = useUniquePath ? DataSegmentPusher.generateUniquePath() + "_" : "";
      final Path outIndexFile = new Path(StringUtils.format(
          "%s/%s/%d_%sindex.zip",
          fullyQualifiedStorageDirectory.get(),
          storageDir,
          segment.getShardSpec().getPartitionNum(),
          uniquePrefix
      ));

      dataSegment = segment.withLoadSpec(makeLoadSpec(outIndexFile.toUri()))
                           .withSize(size)
                           .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir));

      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(outIndexFile.getParent());
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
            "Unable to rename temp file [%s] to segment path [%s], it may have already been pushed by a replica task.",
            from,
            to
        );
      } else {
        throw new IOE("Failed to rename temp file [%s] and final segment path [%s] is not present.", from, to);
      }
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    return ImmutableMap.of("type", "hdfs", "path", finalIndexZipFilePath.toString());
  }

  /**
   * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
   * path names. So we format paths differently for HDFS.
   */

  @Override
  public String getStorageDir(DataSegment segment, boolean useUniquePath)
  {
    // This is only called by HdfsDataSegmentPusher.push(), which will always set useUniquePath to false since any
    // 'uniqueness' will be applied not to the directory but to the filename along with the shard number. This is done
    // to avoid performance issues due to excessive HDFS directories. Hence useUniquePath is ignored here and we
    // expect it to be false.
    Preconditions.checkArgument(
        !useUniquePath,
        "useUniquePath must be false for HdfsDataSegmentPusher.getStorageDir()"
    );

    return JOINER.join(
        segment.getDataSource(),
        StringUtils.format(
            "%s_%s",
            segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
            segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
        ),
        segment.getVersion().replace(':', '_')
    );
  }

  @Override
  public String makeIndexPathName(DataSegment dataSegment, String indexName)
  {
    // This is only called from Hadoop batch which doesn't require unique segment paths so set useUniquePath=false
    return StringUtils.format(
        "./%s/%d_%s",
        this.getStorageDir(dataSegment, false),
        dataSegment.getShardSpec().getPartitionNum(),
        indexName
    );
  }
}
