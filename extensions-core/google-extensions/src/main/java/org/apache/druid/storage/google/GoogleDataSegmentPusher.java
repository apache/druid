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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class GoogleDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(GoogleDataSegmentPusher.class);

  private final GoogleStorage storage;
  private final GoogleAccountConfig config;

  @Inject
  public GoogleDataSegmentPusher(
      final GoogleStorage storage,
      final GoogleAccountConfig config
  )
  {
    this.storage = storage;
    this.config = config;
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
    return StringUtils.format("gs://%s/%s", config.getBucket(), config.getPrefix());
  }

  @Override
  public List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return ImmutableList.of("druid.google");
  }

  public void insert(final File file, final String contentType, final String path)
      throws IOException
  {
    log.debug("Inserting [%s] to [%s]", file, path);
    try {
      RetryUtils.retry(
          (RetryUtils.Task<Void>) () -> {
            storage.insert(config.getBucket(), path, new FileContent(contentType, file));
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

    final int version = SegmentUtils.getVersionFromDir(indexFilesDir);
    File indexFile = null;

    try {
      indexFile = File.createTempFile("index", ".zip");
      final long indexSize = CompressionUtils.zip(indexFilesDir, indexFile);
      final String storageDir = this.getStorageDir(segment, useUniquePath);
      final String indexPath = buildPath(storageDir + "/" + "index.zip");

      final DataSegment outSegment = segment
          .withSize(indexSize)
          .withLoadSpec(makeLoadSpec(config.getBucket(), indexPath))
          .withBinaryVersion(version);

      insert(indexFile, "application/zip", indexPath);

      return outSegment;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      if (indexFile != null) {
        log.debug("Deleting file [%s]", indexFile);
        indexFile.delete();
      }
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
