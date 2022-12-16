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
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.UUID;

public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  public static final String INDEX_DIR = "index";
  public static final String INDEX_ZIP_FILENAME = "index.zip";

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
      for (File file : dataSegmentFile.listFiles()) {
        size += file.length();
      }

      return segment.withLoadSpec(makeLoadSpec(outDir.toURI()))
                    .withSize(size)
                    .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile));
    } else if (config.isZip()) {
      return pushZip(dataSegmentFile, outDir, segment);
    } else {
      return pushNoZip(dataSegmentFile, outDir, segment);
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
        throw new IOE("Failed to rename [%s] to [%s]", tmpIndexFile, indexFileTarget);
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

      try {
        Files.move(tmpSegmentDir.toPath(), segmentDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
      }
      catch (IOException e) {
        if (segmentDir.exists()) {
          // Move old directory out of the way, then try again. This makes the latest push win when we push to the
          // same directory twice, so behavior is compatible with the zip style of pushing.
          Files.move(
              segmentDir.toPath(),
              new File(outDir, StringUtils.format("%s_old_%s", INDEX_DIR, UUID.randomUUID())).toPath(),
              StandardCopyOption.ATOMIC_MOVE
          );

          Files.move(tmpSegmentDir.toPath(), segmentDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
        }
      }

      return baseSegment.withLoadSpec(makeLoadSpec(new File(outDir, INDEX_DIR).toURI()))
                        .withSize(size);
    }
    finally {
      FileUtils.deleteDirectory(tmpSegmentDir);
    }
  }
}
