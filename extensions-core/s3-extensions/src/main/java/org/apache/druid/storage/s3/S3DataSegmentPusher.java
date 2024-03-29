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

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class S3DataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3DataSegmentPusher.class);

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentPusher(
      ServerSideEncryptingAmazonS3 s3Client,
      S3DataSegmentPusherConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
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
    return pushToPath(indexFilesDir, inSegment, getStorageDir(inSegment, useUniquePath));
  }

  @Override
  public DataSegment pushToPath(File indexFilesDir, DataSegment inSegment, String storageDirSuffix) throws IOException
  {
    final String s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), storageDirSuffix);
    log.debug("Copying segment[%s] to S3 at location[%s]", inSegment.getId(), s3Path);

    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

    final DataSegment outSegment = inSegment.withSize(indexSize)
                                            .withLoadSpec(makeLoadSpec(config.getBucket(), s3Path))
                                            .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

    try {
      return S3Utils.retryS3Operation(
          () -> {
            S3Utils.uploadFileIfPossible(s3Client, config.getDisableAcl(), config.getBucket(), s3Path, zipOutFile);

            return outSegment;
          }
      );
    }
    catch (AmazonServiceException e) {
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
      throw new IOException(e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      log.debug("Deleting temporary cached index.zip");
      zipOutFile.delete();
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
        config.isUseS3aSchema() ? "s3a" : "s3n"
    );
  }

}
