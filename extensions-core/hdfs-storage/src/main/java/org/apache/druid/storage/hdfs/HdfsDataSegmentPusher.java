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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopFsWrapper;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

/**
 *
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private static final CompressionUtils.Format DEFAULT_COMPRESSION_FORMAT = CompressionUtils.Format.ZIP;

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  @Nullable
  private final ServiceEmitter emitter;
  private final CompressionUtils.Format compressionFormat;

  // We lazily initialize fullQualifiedStorageDirectory to avoid potential issues with Hadoop namenode HA.
  // Please see https://github.com/apache/druid/pull/5684
  private final Supplier<String> fullyQualifiedStorageDirectory;

  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      @Hdfs Configuration hadoopConfig
  )
  {
    this(config, hadoopConfig, null, new DeepStorageSegmentConfig());
  }

  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      @Hdfs Configuration hadoopConfig,
      DeepStorageSegmentConfig deepStorageConfig
  )
  {
    this(config, hadoopConfig, null, deepStorageConfig);
  }

  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      @Hdfs Configuration hadoopConfig,
      @Nullable ServiceEmitter emitter
  )
  {
    this(config, hadoopConfig, emitter, new DeepStorageSegmentConfig());
  }

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      @Hdfs Configuration hadoopConfig,
      @Nullable ServiceEmitter emitter,
      DeepStorageSegmentConfig deepStorageConfig
  )
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.emitter = emitter;
    final CompressionUtils.Format format = deepStorageConfig.getEffectiveCompressionFormat(DEFAULT_COMPRESSION_FORMAT);
    if (format != CompressionUtils.Format.ZIP && format != CompressionUtils.Format.LZ4 && format != CompressionUtils.Format.NONE) {
      throw new IAE("HDFS deep storage does not support compressionFormat [%s]. Supported: zip, lz4, none.", format);
    }
    this.compressionFormat = format;
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

  @Override
  public DataSegment push(final File inDir, final DataSegment segment, final boolean useUniquePath) throws IOException
  {
    // For HDFS, useUniquePath does not affect the directory tree but instead affects the filename, which is of the form
    // '{partitionNum}_index.zip' without unique paths and '{partitionNum}_{UUID}_index.zip' with unique paths.
    final String storageDir = this.getStorageDir(segment, false);

    final String uniquePrefix = useUniquePath ? DataSegmentPusher.generateUniquePath() + "_" : "";
    final String outIndexFilePath = StringUtils.format(
        "%s/%s/%d_%sindex%s",
        fullyQualifiedStorageDirectory.get(),
        storageDir,
        segment.getShardSpec().getPartitionNum(),
        uniquePrefix,
        compressionFormat.getSuffix()
    );

    return pushToFilePathWithRetry(inDir, segment, outIndexFilePath);
  }

  @Override
  public DataSegment pushToPath(File inDir, DataSegment segment, String storageDirSuffix) throws IOException
  {
    final String outIndexFilePath;
    if (storageDirSuffix.endsWith("index.zip") || storageDirSuffix.endsWith("index.lz4")
        || (compressionFormat == CompressionUtils.Format.NONE && storageDirSuffix.endsWith("index"))) {
      outIndexFilePath = StringUtils.format("%s/%s", fullyQualifiedStorageDirectory.get(), storageDirSuffix);
    } else {
      outIndexFilePath = StringUtils.format(
          "%s/%s/%d_index%s",
          fullyQualifiedStorageDirectory.get(),
          storageDirSuffix.replace(':', '_'),
          segment.getShardSpec().getPartitionNum(),
          compressionFormat.getSuffix()
      );
    }

    return pushToFilePathWithRetry(inDir, segment, outIndexFilePath);
  }

  private DataSegment pushToFilePathWithRetry(File inDir, DataSegment segment, String outIndexFilePath)
      throws IOException
  {
    // Retry any HDFS errors that occur, up to 5 times.
    try {
      final long startTime = System.currentTimeMillis();

      final DataSegment result = RetryUtils.retry(
          () -> pushToFilePath(inDir, segment, outIndexFilePath),
          exception -> exception instanceof Exception,
          5
      );

      emitMetrics(result.getSize(), System.currentTimeMillis() - startTime);

      return result;
    }
    catch (Exception e) {
      Throwables.throwIfInstanceOf(e, IOException.class);
      Throwables.throwIfInstanceOf(e, RuntimeException.class);
      throw new RuntimeException(e);
    }
  }

  private void emitMetrics(long size, long duration)
  {
    if (emitter == null) {
      return;
    }

    final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
    metricBuilder.setDimension("format", compressionFormat);
    emitter.emit(metricBuilder.setMetric("hdfs/push/size", size));
    emitter.emit(metricBuilder.setMetric("hdfs/push/duration", duration));
  }

  private DataSegment pushToFilePath(File inDir, DataSegment segment, String outIndexFilePath) throws IOException
  {
    if (compressionFormat == CompressionUtils.Format.NONE) {
      return pushToFilePathNone(inDir, segment, outIndexFilePath);
    }
    log.debug(
        "Copying segment[%s] to HDFS at location[%s]",
        segment.getId(),
        outIndexFilePath
    );
    Path tmpIndexFile = new Path(StringUtils.format(
        "%s/%s/%s/%s_index%s",
        fullyQualifiedStorageDirectory.get(),
        segment.getDataSource(),
        UUIDUtils.generateUuid(),
        segment.getShardSpec().getPartitionNum(),
        compressionFormat.getSuffix()
    ));
    FileSystem fs = tmpIndexFile.getFileSystem(hadoopConfig);

    fs.mkdirs(tmpIndexFile.getParent());
    log.debug("Compressing files from[%s] to [%s]", inDir, tmpIndexFile);

    final long size;
    final DataSegment dataSegment;
    try {
      try (FSDataOutputStream out = fs.create(tmpIndexFile)) {
        size = compressionFormat.compressDirectory(inDir, out);
      }
      final Path outIndexFile = new Path(outIndexFilePath);
      dataSegment = segment.withLoadSpec(makeLoadSpec(outIndexFile.toUri()))
                           .withSize(size)
                           .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir));

      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(outIndexFile.getParent());
      copyFilesWithChecks(fs, tmpIndexFile, outIndexFile);
    }
    catch (IOException e) {
      log.error(e, "Failed to push segment[%s] to HDFS at location[%s]", segment.getId(), outIndexFilePath);
      throw e;
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

  private DataSegment pushToFilePathNone(File inDir, DataSegment segment, String outIndexDirPath) throws IOException
  {
    log.debug(
        "Copying segment[%s] files to HDFS at location[%s]",
        segment.getId(),
        outIndexDirPath
    );
    final Path tmpIndexDir = new Path(StringUtils.format(
        "%s/%s/%s/%s_index",
        fullyQualifiedStorageDirectory.get(),
        segment.getDataSource(),
        UUIDUtils.generateUuid(),
        segment.getShardSpec().getPartitionNum()
    ));
    final FileSystem fs = tmpIndexDir.getFileSystem(hadoopConfig);

    fs.mkdirs(tmpIndexDir);
    log.debug("Copying flat segment files from[%s] to [%s]", inDir, tmpIndexDir);

    final File[] files = inDir.listFiles();
    if (files == null) {
      throw new IOE("Cannot list directory [%s]", inDir);
    }

    final DataSegment dataSegment;
    try {
      long size = 0;
      for (final File file : files) {
        if (file.isFile()) {
          size += file.length();
          final Path targetPath = new Path(tmpIndexDir, file.getName());
          try (
              final InputStream in = new FileInputStream(file);
              final FSDataOutputStream out = fs.create(targetPath)
          ) {
            ByteStreams.copy(in, out);
          }
        } else {
          // Segment directories are expected to be flat.
          throw new IOE("Unexpected subdirectory [%s]", file.getName());
        }
      }

      final Path outIndexDir = new Path(outIndexDirPath);
      dataSegment = segment.withLoadSpec(makeLoadSpec(outIndexDir.toUri()))
                           .withSize(size)
                           .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir));

      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(outIndexDir.getParent());
      copyFilesWithChecks(fs, tmpIndexDir, outIndexDir);
    }
    catch (IOException e) {
      log.error(e, "Failed to push segment[%s] to HDFS at location[%s]", segment.getId(), outIndexDirPath);
      throw e;
    }
    finally {
      try {
        if (fs.exists(tmpIndexDir.getParent()) && !fs.delete(tmpIndexDir.getParent(), true)) {
          log.error("Failed to delete temp directory[%s]", tmpIndexDir.getParent());
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to delete temp directory[%s]", tmpIndexDir.getParent());
      }
    }

    return dataSegment;
  }

  public CompressionUtils.Format getCompressionFormat()
  {
    return compressionFormat;
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
}
