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

import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;

/**
 * Used for Reading segment files stored in Azure based deep storage
 */
public class AzureDataSegmentPuller
{
  private static final Logger log = new Logger(AzureDataSegmentPuller.class);

  private final AzureByteSourceFactory byteSourceFactory;
  private final AzureStorage azureStorage;

  private final AzureAccountConfig azureAccountConfig;

  @Inject
  public AzureDataSegmentPuller(
      AzureByteSourceFactory byteSourceFactory,
      @Global AzureStorage azureStorage,
      AzureAccountConfig azureAccountConfig
  )
  {
    this.byteSourceFactory = byteSourceFactory;
    this.azureStorage = azureStorage;
    this.azureAccountConfig = azureAccountConfig;
  }

  FileUtils.FileCopyResult getSegmentFiles(
      final String containerName,
      final String blobPath,
      final File outDir
  )
      throws SegmentLoadingException
  {
    try {
      FileUtils.mkdirp(outDir);

      log.info(
          "Loading container: [%s], with blobPath: [%s] and outDir: [%s]", containerName, blobPath, outDir
      );

      final String actualBlobPath = AzureUtils.maybeRemoveAzurePathPrefix(blobPath, azureAccountConfig.getBlobStorageEndpoint());

      final ByteSource byteSource = byteSourceFactory.create(containerName, actualBlobPath, azureStorage);
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource,
          outDir,
          AzureUtils.AZURE_RETRY,
          false
      );

      log.info("Loaded %d bytes from [%s] to [%s]", result.size(), actualBlobPath, outDir.getAbsolutePath());
      return result;
    }
    catch (IOException e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(),
            blobPath
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }
}
