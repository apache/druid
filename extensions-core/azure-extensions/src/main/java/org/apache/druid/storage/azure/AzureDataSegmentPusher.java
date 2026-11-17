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

package org.apache.druid.storage.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

/**
 * Used for writing segment files to Azure based deep storage
 */
public class AzureDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(AzureDataSegmentPusher.class);

  /**
   * Originally Azure always wrote segments zipped, so that is the default when neither property is set.
   */
  private static final CompressionUtils.Format DEFAULT_FORMAT = CompressionUtils.Format.ZIP;

  private final AzureStorage azureStorage;
  private final AzureAccountConfig accountConfig;
  private final AzureDataSegmentConfig segmentConfig;
  private final boolean zip;

  @Inject
  public AzureDataSegmentPusher(
      @Global AzureStorage azureStorage,
      AzureAccountConfig accountConfig,
      AzureDataSegmentConfig segmentConfig,
      DeepStorageSegmentConfig deepStorageConfig
  )
  {
    this.azureStorage = azureStorage;
    this.accountConfig = accountConfig;
    this.segmentConfig = segmentConfig;
    this.zip = deepStorageConfig.getEffectiveCompressionFormat(
        DEFAULT_FORMAT,
        CompressionUtils.Format.ZIP,
        CompressionUtils.Format.NONE
    ) == CompressionUtils.Format.ZIP;
  }

  @Override
  public String getStorageDir(DataSegment dataSegment, boolean useUniquePath)
  {
    String seg = JOINER.join(
        dataSegment.getDataSource(),
        StringUtils.format(
            "%s_%s",
            // Use ISODateTimeFormat.basicDateTime() format, to avoid using colons in file path.
            dataSegment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
            dataSegment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
        ),
        dataSegment.getVersion().replace(':', '_'),
        dataSegment.getShardSpec().getPartitionNum(),
        useUniquePath ? DataSegmentPusher.generateUniquePath() : null
    );

    log.info("DataSegment Suffix: [%s]", seg);

    // Replace colons with underscores, since they are not supported through wasb:// prefix
    return seg;
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    log.info("Uploading [%s] to Azure.", indexFilesDir);
    return pushToPath(indexFilesDir, segment, getStorageDir(segment, useUniquePath));
  }

  @Override
  public DataSegment pushToPath(File indexFilesDir, DataSegment segment, String storageDirSuffix) throws IOException
  {
    String prefix = segmentConfig.getPrefix();
    boolean prefixIsNullOrEmpty = org.apache.commons.lang3.StringUtils.isEmpty(prefix);
    final String azureBasePath = JOINER.join(
        prefixIsNullOrEmpty ? null : StringUtils.maybeRemoveTrailingSlash(prefix),
        "_druid_generations",
        storageDirSuffix,
        UUIDUtils.generateUuid()
    );

    final int binaryVersion = SegmentUtils.getVersionFromDir(indexFilesDir);

    try {
      if (zip && IndexIO.isExternalCompressionSupported(binaryVersion)) {
        return pushZip(indexFilesDir, segment, binaryVersion, azureBasePath);
      } else {
        return pushNoZip(indexFilesDir, segment, binaryVersion, azureBasePath);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private DataSegment pushZip(
      File indexFilesDir,
      DataSegment segment,
      int binaryVersion,
      String azureBasePath
  ) throws IOException
  {
    File zipOutFile = null;

    try {
      final File outFile = zipOutFile = Files.createTempFile("index", ".zip").toFile();
      final long size = CompressionUtils.zip(indexFilesDir, zipOutFile);

      return uploadDataSegment(segment, binaryVersion, size, outFile, getZipBlobPath(azureBasePath));
    }
    catch (IOException | RuntimeException e) {
      cleanupGeneration(azureBasePath);
      throw e;
    }
    finally {
      if (zipOutFile != null) {
        log.info("Deleting zipped index File[%s]", zipOutFile);
        zipOutFile.delete();
      }
    }
  }

  /**
   * Uploads the segment files as they are, one blob per file, under {@code azureBasePath}. The resulting loadSpec
   * blobPath is the directory itself, with a trailing slash to tell {@link AzureDataSegmentPuller} and
   * {@link AzureDataSegmentKiller} that it names a directory of files rather than a single blob.
   */
  private DataSegment pushNoZip(
      File indexFilesDir,
      DataSegment segment,
      int binaryVersion,
      String azureBasePath
  ) throws IOException
  {
    boolean uploadAttempted = false;
    try {
      final File[] files = indexFilesDir.listFiles();
      if (files == null) {
        throw new IOE("Cannot list directory [%s]", indexFilesDir);
      }

      final String azureDirPath = azureBasePath + "/";
      long size = 0;
      for (final File file : files) {
        if (file.isFile()) {
          size += file.length();
          final String blobPath = azureDirPath + file.getName();
          uploadAttempted = true;
          azureStorage.uploadBlockBlob(file, segmentConfig.getContainer(), blobPath, accountConfig.getMaxTries());
        } else {
          throw new IOE("Unexpected subdirectory [%s]", file.getName());
        }
      }

      final boolean rangeable = binaryVersion == IndexIO.V10_VERSION;
      return segment.withSize(size)
                    .withLoadSpec(makeLoadSpec(azureDirPath, rangeable))
                    .withBinaryVersion(binaryVersion);
    }
    catch (IOException | RuntimeException e) {
      if (uploadAttempted) {
        cleanupGeneration(azureBasePath);
      }
      throw e;
    }
  }

  private void cleanupGeneration(final String generationPath)
  {
    try {
      azureStorage.emptyCloudBlobDirectory(segmentConfig.getContainer(), generationPath, accountConfig.getMaxTries());
    }
    catch (Exception cleanupException) {
      log.warn(cleanupException, "Unable to clean up failed segment generation[%s]", generationPath);
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI uri)
  {
    return makeLoadSpec(uri.toString());
  }

  /**
   * Path of the {@code index.zip} blob for a segment, relative to {@link AzureDataSegmentConfig#getPrefix()}.
   */
  @VisibleForTesting
  String getAzurePath(final DataSegment segment, final boolean useUniquePath)
  {
    return getZipBlobPath(this.getStorageDir(segment, useUniquePath));
  }

  private static String getZipBlobPath(final String azureBasePath)
  {
    return StringUtils.format("%s/%s", azureBasePath, AzureStorageDruidModule.INDEX_ZIP_FILE_NAME);
  }

  @VisibleForTesting
  DataSegment uploadDataSegment(
      DataSegment segment,
      final int binaryVersion,
      final long size,
      final File compressedSegmentData,
      final String azurePath
  )
      throws BlobStorageException, IOException
  {
    azureStorage.uploadBlockBlob(compressedSegmentData, segmentConfig.getContainer(), azurePath, accountConfig.getMaxTries());

    final DataSegment outSegment = segment
        .withSize(size)
        .withLoadSpec(this.makeLoadSpec(azurePath))
        .withBinaryVersion(binaryVersion);

    log.debug("Deleting file [%s]", compressedSegmentData);
    compressedSegmentData.delete();

    return outSegment;
  }

  private Map<String, Object> makeLoadSpec(String prefix)
  {
    return ImmutableMap.of(
        "type",
        AzureStorageDruidModule.SCHEME,
        "containerName",
        segmentConfig.getContainer(),
        "blobPath",
        prefix
    );
  }

  /**
   * Variant that stamps {@link AzureLoadSpec#RANGEABLE} so {@link AzureLoadSpec#openRangeReader()} can decide
   * range-read eligibility. Used by the unzipped push path where the binary version is known at write time.
   */
  private Map<String, Object> makeLoadSpec(String prefix, boolean rangeable)
  {
    return ImmutableMap.of(
        "type",
        AzureStorageDruidModule.SCHEME,
        "containerName",
        segmentConfig.getContainer(),
        "blobPath",
        prefix,
        AzureLoadSpec.RANGEABLE,
        rangeable
    );
  }
}
