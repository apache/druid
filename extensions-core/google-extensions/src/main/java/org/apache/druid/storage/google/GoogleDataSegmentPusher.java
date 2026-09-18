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
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GoogleDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(GoogleDataSegmentPusher.class);

  static final String INDEX_ZIP_FILE_NAME = "index.zip";

  /**
   * Originally Google deep storage always wrote segments zipped, so that is what {@code druid.storage.zip} falls back
   * to.
   */
  private static final boolean DEFAULT_ZIP = true;

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
    this.zip = deepStorageConfig.isZip(DEFAULT_ZIP);
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
    final String basePath = buildPath(storageDirSuffix);

    try {
      if (zip) {
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
    finally {
      if (indexFile != null) {
        log.debug("Deleting file [%s]", indexFile);
        indexFile.delete();
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
    final File[] files = indexFilesDir.listFiles();
    if (files == null) {
      throw new IOE("Cannot list directory [%s]", indexFilesDir);
    }

    final String dirPath = basePath + "/";
    final Set<String> pushedPaths = new HashSet<>();

    long size = 0;
    for (final File file : files) {
      if (file.isFile()) {
        size += file.length();
        final String path = dirPath + file.getName();
        insert(file, "application/octet-stream", path);
        pushedPaths.add(path);
      } else {
        // Segment directories are expected to be flat.
        throw new IOE("Unexpected subdirectory [%s]", file.getName());
      }
    }

    deleteStaleObjects(dirPath, pushedPaths);

    return segment.withSize(size)
                  .withLoadSpec(makeLoadSpec(config.getBucket(), dirPath))
                  .withBinaryVersion(version);
  }

  /**
   * Removes everything under {@code dirPath} that this push did not write.
   * <p>
   * A zipped push replaces the previous segment outright, because one {@code index.zip} object overwrites another, and
   * {@link #push} with {@code useUniquePath = false} is expected to replace a previous push the same way. Uploading
   * file by file only overwrites the names the new segment happens to share, so without this a re-push could leave
   * behind objects of whatever was there before: a stale {@code index.zip} from a zipped push, or smoosh chunks from a
   * larger prior v9 segment.
   * <p>
   * Note that this makes an unzipped push require permission to delete objects under the segment path, which a zipped
   * push does not.
   */
  private void deleteStaleObjects(final String dirPath, final Set<String> pushedPaths) throws IOException
  {
    try {
      GoogleUtils.deleteObjectsInPath(
          storage,
          inputDataConfig,
          config.getBucket(),
          dirPath,
          object -> !pushedPaths.contains(object.getName())
      );
    }
    catch (Exception e) {
      throw new IOE(
          e,
          "Could not remove objects left under [gs://%s/%s] by a previous push, which would be loaded as part of this"
          + " segment",
          config.getBucket(),
          dirPath
      );
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

}
