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

package org.apache.druid.storage.google;

import com.google.api.client.http.FileContent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

public class GoogleDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(GoogleDataSegmentPusher.class);

  static final String INDEX_ZIP_FILE_NAME = "index.zip";

  /**
   * Originally Google deep storage always wrote segments zipped, so that is the default when neither property is set.
   */
  private static final CompressionUtils.Format DEFAULT_FORMAT = CompressionUtils.Format.ZIP;

  private final GoogleStorage storage;
  private final GoogleAccountConfig config;
  private final GoogleInputDataConfig inputDataConfig;
  private final boolean zip;

  @Inject
  public GoogleDataSegmentPusher(
      final GoogleStorage storage,
      final GoogleAccountConfig config,
      final GoogleInputDataConfig inputDataConfig,
      final DeepStorageSegmentConfig deepStorageConfig
  )
  {
    this.storage = storage;
    this.config = config;
    this.inputDataConfig = inputDataConfig;
    this.zip = deepStorageConfig.getEffectiveCompressionFormat(
        DEFAULT_FORMAT,
        CompressionUtils.Format.ZIP,
        CompressionUtils.Format.NONE
    ) == CompressionUtils.Format.ZIP;
  }

  public void insert(final File file, final String contentType, final String path)
      throws IOException
  {
    log.debug("Inserting [%s] to [%s]", file, path);
    try {
      RetryUtils.retry(
          (RetryUtils.Task<Void>) () -> {
            storage.insert(config.getBucket(), path, new FileContent(contentType, file), null);
            return null;
          },
          GoogleUtils::isRetryable,
          1,
          5
      );
    }
    catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RE(e, "Failed to upload [%s] to [%s]", file, path);
    }
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    log.debug("Uploading [%s] to Google.", indexFilesDir);
    final String storageDir = this.getStorageDir(segment, useUniquePath);
    return pushToPath(indexFilesDir, segment, storageDir);
  }

  @Override
  public DataSegment pushToPath(File indexFilesDir, DataSegment segment, String storageDirSuffix) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(indexFilesDir);
    final String basePath = buildPath("_druid_generations/" + storageDirSuffix + "/" + UUIDUtils.generateUuid());

    try {
      if (zip && IndexIO.isExternalCompressionSupported(version)) {
        return pushZip(indexFilesDir, segment, version, basePath);
      } else {
        return pushNoZip(indexFilesDir, segment, version, basePath);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private DataSegment pushZip(File indexFilesDir, DataSegment segment, int version, String basePath)
      throws IOException
  {
    File indexFile = null;

    try {
      indexFile = Files.createTempFile("index", ".zip").toFile();
      final long indexSize = CompressionUtils.zip(indexFilesDir, indexFile);
      final String indexPath = basePath + "/" + INDEX_ZIP_FILE_NAME;

      final DataSegment outSegment = segment
          .withSize(indexSize)
          .withLoadSpec(makeLoadSpec(config.getBucket(), indexPath))
          .withBinaryVersion(version);

      insert(indexFile, "application/zip", indexPath);

      return outSegment;
    }
    catch (IOException | RuntimeException e) {
      cleanupGeneration(basePath);
      throw e;
    }
    finally {
      if (indexFile != null) {
        log.debug("Deleting file [%s]", indexFile);
        if (!indexFile.delete()) {
          log.warn("Failed to delete temporary file [%s]", indexFile.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Uploads the segment files as they are, one object per file, under {@code basePath}. The resulting loadSpec path is
   * the directory itself, with a trailing slash to tell {@link GoogleDataSegmentPuller} and
   * {@link GoogleDataSegmentKiller} that it names a directory of files rather than a single object.
   */
  private DataSegment pushNoZip(File indexFilesDir, DataSegment segment, int version, String basePath)
      throws IOException
  {
    try {
      final File[] files = indexFilesDir.listFiles();
      if (files == null) {
        throw new IOE("Cannot list directory [%s]", indexFilesDir);
      }

      final String dirPath = basePath + "/";
      long size = 0;
      for (final File file : files) {
        if (file.isFile()) {
          size += file.length();
          final String path = dirPath + file.getName();
          insert(file, "application/octet-stream", path);
        } else {
          throw new IOE("Unexpected subdirectory [%s]", file.getName());
        }
      }

      final boolean rangeable = version == IndexIO.V10_VERSION;
      return segment.withSize(size)
                    .withLoadSpec(makeLoadSpec(config.getBucket(), dirPath, rangeable))
                    .withBinaryVersion(version);
    }
    catch (IOException | RuntimeException e) {
      cleanupGeneration(basePath);
      throw e;
    }
  }

  private void cleanupGeneration(final String generationPath)
  {
    try {
      GoogleUtils.deleteObjectsInPath(storage, inputDataConfig, config.getBucket(), generationPath, ignored -> true);
    }
    catch (Exception cleanupException) {
      log.warn(cleanupException, "Unable to clean up failed segment generation[%s]", generationPath);
    }
  }

  @VisibleForTesting
  String buildPath(final String path)
  {
    if (!Strings.isNullOrEmpty(config.getPrefix())) {
      return config.getPrefix() + "/" + path;
    } else {
      return path;
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    // remove the leading "/"
    return makeLoadSpec(config.getBucket(), finalIndexZipFilePath.getPath().substring(1));
  }

  private Map<String, Object> makeLoadSpec(String bucket, String path)
  {
    return ImmutableMap.of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", bucket,
        "path", path
    );
  }

  /**
   * Variant that stamps {@link GoogleLoadSpec#RANGEABLE} so {@link GoogleLoadSpec#openRangeReader()} can decide
   * range-read eligibility. Used by the unzipped push path where the binary version is known at write time.
   */
  private Map<String, Object> makeLoadSpec(String bucket, String path, boolean rangeable)
  {
    return ImmutableMap.of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", bucket,
        "path", path,
        GoogleLoadSpec.RANGEABLE, rangeable
    );
  }

}
