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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Used for writing segment files to Azure based deep storage
 */
public class AzureDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(AzureDataSegmentPusher.class);
  static final List<String> ALLOWED_PROPERTY_PREFIXES_FOR_HADOOP = ImmutableList.of("druid.azure");
  private final AzureStorage azureStorage;
  private final AzureAccountConfig accountConfig;
  private final AzureDataSegmentConfig segmentConfig;

  @Inject
  public AzureDataSegmentPusher(
      @Global AzureStorage azureStorage,
      AzureAccountConfig accountConfig,
      AzureDataSegmentConfig segmentConfig
  )
  {
    this.azureStorage = azureStorage;
    this.accountConfig = accountConfig;
    this.segmentConfig = segmentConfig;
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
    String prefix = segmentConfig.getPrefix();
    boolean prefixIsNullOrEmpty = org.apache.commons.lang.StringUtils.isEmpty(prefix);
    String hadoopPath = StringUtils.format(
        "%s://%s@%s.%s/%s",
        AzureUtils.AZURE_STORAGE_HADOOP_PROTOCOL,
        segmentConfig.getContainer(),
        accountConfig.getAccount(),
        accountConfig.getBlobStorageEndpoint(),
        prefixIsNullOrEmpty ? "" : StringUtils.maybeRemoveTrailingSlash(prefix) + '/'
    );

    log.info("Using Azure blob storage Hadoop path: %s", hadoopPath);

    return hadoopPath;
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
  public List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return ALLOWED_PROPERTY_PREFIXES_FOR_HADOOP;
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment segment, final boolean useUniquePath)
      throws IOException
  {
    log.info("Uploading [%s] to Azure.", indexFilesDir);
    final String azurePathSuffix = getAzurePath(segment, useUniquePath);
    return pushToPath(indexFilesDir, segment, azurePathSuffix);
  }

  @Override
  public DataSegment pushToPath(File indexFilesDir, DataSegment segment, String storageDirSuffix) throws IOException
  {
    String prefix = segmentConfig.getPrefix();
    boolean prefixIsNullOrEmpty = org.apache.commons.lang.StringUtils.isEmpty(prefix);
    final String azurePath = JOINER.join(
        prefixIsNullOrEmpty ? null : StringUtils.maybeRemoveTrailingSlash(prefix),
        storageDirSuffix
    );

    final int binaryVersion = SegmentUtils.getVersionFromDir(indexFilesDir);
    File zipOutFile = null;

    try {
      final File outFile = zipOutFile = File.createTempFile("index", ".zip");
      final long size = CompressionUtils.zip(indexFilesDir, zipOutFile);

      return uploadDataSegment(segment, binaryVersion, size, outFile, azurePath);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      if (zipOutFile != null) {
        log.info("Deleting zipped index File[%s]", zipOutFile);
        zipOutFile.delete();
      }
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI uri)
  {
    return makeLoadSpec(uri.toString());
  }

  @VisibleForTesting
  String getAzurePath(final DataSegment segment, final boolean useUniquePath)
  {
    final String storageDir = this.getStorageDir(segment, useUniquePath);

    return StringUtils.format("%s/%s", storageDir, AzureStorageDruidModule.INDEX_ZIP_FILE_NAME);

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
}
