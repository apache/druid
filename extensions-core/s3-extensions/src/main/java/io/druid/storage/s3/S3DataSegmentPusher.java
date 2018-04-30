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

package io.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class S3DataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3DataSegmentPusher.class);

  private final AmazonS3 s3Client;
  private final S3DataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3DataSegmentPusher(AmazonS3 s3Client, S3DataSegmentPusherConfig config, ObjectMapper jsonMapper)
  {
    this.s3Client = s3Client;
    this.config = config;
    this.jsonMapper = jsonMapper;

    log.info("Configured S3 as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    if (config.isUseS3aSchema()) {
      return StringUtils.format("s3a://%s/%s", config.getBucket(), config.getBaseKey());
    }
    return StringUtils.format("s3n://%s/%s", config.getBucket(), config.getBaseKey());
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return ImmutableList.of("druid.s3");
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment, final boolean useUniquePath)
      throws IOException
  {
    final String s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), getStorageDir(inSegment, useUniquePath));

    log.info("Copying segment[%s] to S3 at location[%s]", inSegment.getIdentifier(), s3Path);

    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

    final DataSegment outSegment = inSegment.withSize(indexSize)
                                            .withLoadSpec(makeLoadSpec(config.getBucket(), s3Path))
                                            .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

    final File descriptorFile = File.createTempFile("druid", "descriptor.json");
    // Avoid using Guava in DataSegmentPushers because they might be used with very diverse Guava versions in
    // runtime, and because Guava deletes methods over time, that causes incompatibilities.
    Files.write(descriptorFile.toPath(), jsonMapper.writeValueAsBytes(outSegment));

    try {
      return S3Utils.retryS3Operation(
          () -> {
            uploadFileIfPossible(s3Client, config.getBucket(), s3Path, zipOutFile);
            uploadFileIfPossible(
                s3Client,
                config.getBucket(),
                S3Utils.descriptorPathForSegmentPath(s3Path),
                descriptorFile
            );

            return outSegment;
          }
      );
    }
    catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      log.info("Deleting temporary cached index.zip");
      zipOutFile.delete();
      log.info("Deleting temporary cached descriptor.json");
      descriptorFile.delete();
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    // remove the leading "/"
    return makeLoadSpec(finalIndexZipFilePath.getHost(), finalIndexZipFilePath.getPath().substring(1));
  }

  /**
   * Any change in loadSpec need to be reflected {@link io.druid.indexer.JobHelper#getURIFromSegment()}
   */
  @SuppressWarnings("JavadocReference")
  private Map<String, Object> makeLoadSpec(String bucket, String key)
  {
    return ImmutableMap.<String, Object>of(
        "type",
        "s3_zip",
        "bucket",
        bucket,
        "key",
        key,
        "S3Schema",
        config.isUseS3aSchema() ? "s3a" : "s3n"
    );
  }

  private void uploadFileIfPossible(AmazonS3 s3Client, String bucket, String key, File file)
  {
    final PutObjectRequest indexFilePutRequest = new PutObjectRequest(bucket, key, file);

    if (!config.getDisableAcl()) {
      indexFilePutRequest.setAccessControlList(S3Utils.grantFullControlToBucketOwner(s3Client, bucket));
    }
    log.info("Pushing [%s] to bucket[%s] and key[%s].", file, bucket, key);
    s3Client.putObject(indexFilePutRequest);
  }
}
