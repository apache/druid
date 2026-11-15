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

package org.apache.druid.storage.s3;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

public class S3DataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3DataSegmentPusher.class);

  /**
   * Originally S3 always wrote segments zipped, so that is the default when neither property is set.
   */
  private static final CompressionUtils.Format DEFAULT_FORMAT = CompressionUtils.Format.ZIP;

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final S3DataSegmentPusherConfig config;
  private final boolean zip;

  @Inject
  public S3DataSegmentPusher(
      ServerSideEncryptingAmazonS3 s3Client,
      S3DataSegmentPusherConfig config,
      DeepStorageSegmentConfig deepStorageConfig
  )
  {
    this.s3Client = s3Client;
    this.config = config;
    this.zip = deepStorageConfig.getEffectiveCompressionFormat(
        DEFAULT_FORMAT,
        CompressionUtils.Format.ZIP,
        CompressionUtils.Format.NONE
    ) == CompressionUtils.Format.ZIP;
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment, final boolean useUniquePath)
      throws IOException
  {
    return pushToPath(indexFilesDir, inSegment, getStorageDir(inSegment, useUniquePath));
  }

  @Override
  public DataSegment pushToPath(File indexFilesDir, DataSegment inSegment, String storageDirSuffix) throws IOException
  {
    final int binaryVersion = SegmentUtils.getVersionFromDir(indexFilesDir);
    final boolean shouldZip = zip && IndexIO.isExternalCompressionSupported(binaryVersion);
    final String generationPath = S3Utils.constructGenerationBasePath(
        config.getBaseKey(),
        storageDirSuffix,
        UUIDUtils.generateUuid()
    );
    if (shouldZip) {
      final String s3Path = generationPath + "index.zip";
      log.debug("Copying segment[%s] to S3 at location[%s]", inSegment.getId(), s3Path);
      return pushZip(indexFilesDir, inSegment, binaryVersion, s3Path);
    } else {
      log.debug("Copying segment[%s] files to S3 at location[%s]", inSegment.getId(), generationPath);
      return pushNoZip(indexFilesDir, inSegment, binaryVersion, generationPath);
    }
  }

  private DataSegment pushZip(File indexFilesDir, DataSegment baseSegment, int binaryVersion, String s3Path) throws IOException
  {
    final String generationPath = s3Path.substring(0, s3Path.length() - "index.zip".length());
    final File zipOutFile = Files.createTempFile("druid", "index.zip").toFile();
    try {
      final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

      final DataSegment outSegment = baseSegment.withSize(indexSize)
                                                .withLoadSpec(makeLoadSpec(config.getBucket(), s3Path))
                                                .withBinaryVersion(binaryVersion);

      try {
        return S3Utils.retryS3Operation(
            () -> {
              S3Utils.uploadFileIfPossible(s3Client, config.getDisableAcl(), config.getBucket(), s3Path, zipOutFile);

              return outSegment;
            }
        );
      }
      catch (S3Exception e) {
        cleanupGeneration(generationPath);
        throw handlePushServiceException(e, indexSize);
      }
      catch (Exception e) {
        cleanupGeneration(generationPath);
        throw new RuntimeException(e);
      }
    }
    finally {
      log.debug("Deleting temporary cached index.zip");
      if (!zipOutFile.delete()) {
        log.warn("Failed to delete temporary cached index.zip file [%s]", zipOutFile.getAbsolutePath());
      }
    }
  }

  private DataSegment pushNoZip(File indexFilesDir, DataSegment baseSegment, int binaryVersion, String s3Path) throws IOException
  {
    try {
      final File[] files = indexFilesDir.listFiles();
      if (files == null) {
        throw new IOE("Cannot list directory [%s]", indexFilesDir);
      }

      long size = 0;
      for (final File file : files) {
        if (file.isFile()) {
          size += file.length();
          final String key = s3Path + file.getName();

          try {
            S3Utils.retryS3Operation(
                () -> {
                  S3Utils.uploadFileIfPossible(s3Client, config.getDisableAcl(), config.getBucket(), key, file);
                  return file;
                }
            );
          }
          catch (S3Exception e) {
            throw handlePushServiceException(e, file.length());
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }

        } else {
          throw new IOE("Unexpected subdirectory [%s]", file.getName());
        }
      }

      final boolean rangeable = binaryVersion == IndexIO.V10_VERSION;
      return baseSegment.withSize(size)
                        .withLoadSpec(makeLoadSpec(config.getBucket(), s3Path, rangeable))
                        .withBinaryVersion(binaryVersion);
    }
    catch (Exception e) {
      cleanupGeneration(s3Path);
      Throwables.throwIfInstanceOf(e, IOException.class);
      throw new RuntimeException(e);
    }
  }

  private void cleanupGeneration(final String generationPath)
  {
    try {
      S3Utils.deleteObjectsInPath(
          s3Client,
          config.getMaxListingLength(),
          config.getBucket(),
          generationPath,
          ignored -> true
      );
    }
    catch (Exception cleanupException) {
      log.warn(cleanupException, "Unable to clean up failed segment generation[%s]", generationPath);
    }
  }


  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    // remove the leading "/"
    return makeLoadSpec(finalIndexZipFilePath.getHost(), finalIndexZipFilePath.getPath().substring(1));
  }

  /**
   * Any change in loadSpec need to be reflected {@link org.apache.druid.indexer.JobHelper#getURIFromSegment()}
   */
  @SuppressWarnings("JavadocReference")
  private Map<String, Object> makeLoadSpec(String bucket, String key)
  {
    return ImmutableMap.of(
        "type",
        "s3_zip",
        "bucket",
        bucket,
        "key",
        key,
        "S3Schema",
        "s3n"
    );
  }

  /**
   * Variant that stamps {@link S3LoadSpec#RANGEABLE} so {@link S3LoadSpec#openRangeReader()} can decide range-read
   * eligibility. Used by the unzipped push path where the binary version is known at write time.
   */
  private Map<String, Object> makeLoadSpec(String bucket, String key, boolean rangeable)
  {
    return ImmutableMap.of(
        "type",
        "s3_zip",
        "bucket",
        bucket,
        "key",
        key,
        "S3Schema",
        "s3n",
        S3LoadSpec.RANGEABLE,
        rangeable
    );
  }

  private static IOException handlePushServiceException(S3Exception e, long indexSize)
  {
    if (S3Utils.ERROR_ENTITY_TOO_LARGE.equals(S3Utils.getS3ErrorCode(e))) {
      throw DruidException
          .forPersona(DruidException.Persona.USER)
          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
          .build(
              e,
              "Got error[%s] from S3 when uploading segment of size[%,d] bytes. This typically happens when segment "
              + "size is above 5GB. Try reducing your segment size by lowering the target number of rows per "
              + "segment.",
              S3Utils.ERROR_ENTITY_TOO_LARGE,
              indexSize
          );
    }
    return new IOException(e);
  }
}
