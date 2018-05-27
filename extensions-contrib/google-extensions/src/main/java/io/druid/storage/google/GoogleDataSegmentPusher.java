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

package io.druid.storage.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.InputStreamContent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class GoogleDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger LOG = new Logger(GoogleDataSegmentPusher.class);

  private final GoogleStorage storage;
  private final GoogleAccountConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public GoogleDataSegmentPusher(
      final GoogleStorage storage,
      final GoogleAccountConfig config,
      final ObjectMapper jsonMapper
  )
  {
    this.storage = storage;
    this.config = config;
    this.jsonMapper = jsonMapper;

    LOG.info("Configured Google as deep storage");
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

  public File createDescriptorFile(final ObjectMapper jsonMapper, final DataSegment segment)
      throws IOException
  {
    File descriptorFile = File.createTempFile("descriptor", ".json");
    // Avoid using Guava in DataSegmentPushers because they might be used with very diverse Guava versions in
    // runtime, and because Guava deletes methods over time, that causes incompatibilities.
    Files.write(descriptorFile.toPath(), jsonMapper.writeValueAsBytes(segment));
    return descriptorFile;
  }

  public void insert(final File file, final String contentType, final String path)
      throws IOException
  {
    LOG.info("Inserting [%s] to [%s]", file, path);

    FileInputStream fileSteam = new FileInputStream(file);

    InputStreamContent mediaContent = new InputStreamContent(contentType, fileSteam);
    mediaContent.setLength(file.length());

    storage.insert(config.getBucket(), path, mediaContent);
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    LOG.info("Uploading [%s] to Google.", indexFilesDir);

    final int version = SegmentUtils.getVersionFromDir(indexFilesDir);
    File indexFile = null;
    File descriptorFile = null;

    try {
      indexFile = File.createTempFile("index", ".zip");
      final long indexSize = CompressionUtils.zip(indexFilesDir, indexFile);
      final String storageDir = this.getStorageDir(segment, useUniquePath);
      final String indexPath = buildPath(storageDir + "/" + "index.zip");
      final String descriptorPath = buildPath(storageDir + "/" + "descriptor.json");

      final DataSegment outSegment = segment
          .withSize(indexSize)
          .withLoadSpec(makeLoadSpec(config.getBucket(), indexPath))
          .withBinaryVersion(version);

      descriptorFile = createDescriptorFile(jsonMapper, outSegment);

      insert(indexFile, "application/zip", indexPath);
      insert(descriptorFile, "application/json", descriptorPath);

      return outSegment;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      if (indexFile != null) {
        LOG.info("Deleting file [%s]", indexFile);
        indexFile.delete();
      }

      if (descriptorFile != null) {
        LOG.info("Deleting file [%s]", descriptorFile);
        descriptorFile.delete();
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
    return ImmutableMap.<String, Object>of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", bucket,
        "path", path
    );
  }

}
