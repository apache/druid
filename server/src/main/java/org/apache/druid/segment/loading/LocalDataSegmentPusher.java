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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  public static final String INDEX_DIR = "index";
  public static final String INDEX_ZIP_FILENAME = "index.zip";

  /**
   * Local deep storage writes segments as a directory of files by default.
   */
  private static final CompressionUtils.Format DEFAULT_FORMAT = CompressionUtils.Format.NONE;

  private static final Object[] JVM_LOCKS = new Object[64];

  static {
    for (int i = 0; i < JVM_LOCKS.length; i++) {
      JVM_LOCKS[i] = new Object();
    }
  }

  private final LocalDataSegmentPusherConfig config;
  private final CompressionUtils.Format compressionFormat;

  @Inject
  public LocalDataSegmentPusher(LocalDataSegmentPusherConfig config, DeepStorageSegmentConfig deepStorageConfig)
  {
    this.config = config;
    this.compressionFormat = deepStorageConfig.getEffectiveCompressionFormat(
        DEFAULT_FORMAT,
        CompressionUtils.Format.ZIP,
        CompressionUtils.Format.NONE
    );
  }

  private Object getJvmLock(String path)
  {
    return JVM_LOCKS[(path.hashCode() & 0x7FFFFFFF) % JVM_LOCKS.length];
  }

  @Override
  public DataSegment push(final File dataSegmentFile, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    return pushToPath(dataSegmentFile, segment, this.getStorageDir(segment, useUniquePath));
  }

  @Override
  public DataSegment pushToPath(File dataSegmentFile, DataSegment segment, String storageDirSuffix) throws IOException
  {
    final File baseStorageDir = config.getStorageDirectory();
    final File outDir = new File(baseStorageDir, storageDirSuffix);

    log.debug("Copying segment[%s] to local filesystem at location[%s]", segment.getId(), outDir.toString());

    // Add binary version to the DataSegment object.
    segment = segment.withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile));

    if (dataSegmentFile.equals(outDir)) {
      // Input and output directories are the same. Compute size, build a loadSpec, and return.
      long size = 0;
      for (File file : Objects.requireNonNull(dataSegmentFile.listFiles())) {
        size += file.length();
      }

      return segment.withLoadSpec(makeLoadSpec(outDir.toURI()))
                    .withSize(size)
                    .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile));
    }

    FileUtils.mkdirp(outDir);
    final String canonicalPath = outDir.getCanonicalPath();
    final File lockFile = new File(outDir, ".push.lock");

    synchronized (getJvmLock(canonicalPath)) {
      try (FileChannel channel = FileChannel.open(
        lockFile.toPath(),
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE
      );
           FileLock ignored = channel.lock()) {

        DataSegment result;

        if (compressionFormat == CompressionUtils.Format.ZIP
          && SegmentUtils.getVersionFromDir(dataSegmentFile) != IndexIO.V10_VERSION) {
          result = pushZip(dataSegmentFile, outDir, segment);
        } else {
          result = pushNoZip(dataSegmentFile, outDir, segment);
        }

        return result;
      }
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

  private DataSegment pushZip(final File inDir, final File outDir, final DataSegment baseSegment) throws IOException
  {
    final File tmpSegmentDir = new File(config.getStorageDirectory(), makeIntermediateDir());
    final File tmpIndexFile = new File(tmpSegmentDir, INDEX_ZIP_FILENAME);

    log.debug("Creating intermediate directory[%s] for segment[%s].", tmpSegmentDir.toString(), baseSegment.getId());
    FileUtils.mkdirp(tmpSegmentDir);

    try {
      log.debug("Compressing files from[%s] to [%s]", inDir, tmpIndexFile);
      final long size = CompressionUtils.zip(inDir, tmpIndexFile, true);

      FileUtils.mkdirp(outDir);
      final File indexFileTarget = new File(outDir, tmpIndexFile.getName());

      if (!tmpIndexFile.renameTo(indexFileTarget)) {
        Files.move(tmpIndexFile.toPath(), indexFileTarget.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      }

      final File siblingDir = new File(outDir, INDEX_DIR);
      if (siblingDir.exists()) {
        try {
          FileUtils.deleteDirectory(siblingDir);
        }
        catch (Exception e) {
          log.warn(e, "Failed to delete stale sibling directory [%s]", siblingDir);
        }
      }

      return baseSegment.withLoadSpec(makeLoadSpec(new File(outDir, INDEX_ZIP_FILENAME).toURI()))
                        .withSize(size);
    }
    finally {
      FileUtils.deleteDirectory(tmpSegmentDir);
    }
  }

  private DataSegment pushNoZip(final File inDir, final File outDir, final DataSegment baseSegment) throws IOException
  {
    final File tmpSegmentDir = new File(config.getStorageDirectory(), makeIntermediateDir());
    FileUtils.mkdirp(tmpSegmentDir);

    try {
      final File[] files = inDir.listFiles();
      if (files == null) {
        throw new IOE("Cannot list directory [%s]", inDir);
      }

      long size = 0;
      for (final File file : files) {
        if (file.isFile()) {
          size += file.length();
          FileUtils.linkOrCopy(file, new File(tmpSegmentDir, file.getName()));
        } else {
          // Segment directories are expected to be flat.
          throw new IOE("Unexpected subdirectory [%s]", file.getName());
        }
      }

      final File segmentDir = new File(outDir, INDEX_DIR);
      FileUtils.mkdirp(outDir);

      File oldSegmentDir = null;
      if (segmentDir.exists()) {
        oldSegmentDir = new File(outDir, StringUtils.format("%s_old_%s", INDEX_DIR, UUID.randomUUID()));
        Files.move(segmentDir.toPath(), oldSegmentDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
      }

      try {
        Files.move(tmpSegmentDir.toPath(), segmentDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
      }
      catch (IOException e) {
        if (oldSegmentDir != null && oldSegmentDir.exists() && !segmentDir.exists()) {
          try {
            Files.move(oldSegmentDir.toPath(), segmentDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
          }
          catch (IOException restoreEx) {
            e.addSuppressed(restoreEx);
          }
        }
        throw e;
      }

      if (oldSegmentDir != null) {
        try {
          FileUtils.deleteDirectory(oldSegmentDir);
        }
        catch (Exception e) {
          log.warn(e, "Failed to delete stale old directory [%s]", oldSegmentDir);
        }
      }

      final File siblingZip = new File(outDir, INDEX_ZIP_FILENAME);
      if (siblingZip.exists() && !siblingZip.delete()) {
        log.warn("Failed to delete stale sibling file [%s]", siblingZip);
      }

      return baseSegment.withLoadSpec(makeLoadSpec(new File(outDir, INDEX_DIR).toURI()))
                        .withSize(size);
    }
    finally {
      FileUtils.deleteDirectory(tmpSegmentDir);
    }
  }
}
