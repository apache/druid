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
import java.nio.file.Paths;
import java.util.List;

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

      // A trailing slash means the segment was pushed unzipped (druid.storage.zip=false), so the path names a
      // directory of blobs to pull individually rather than a single zip to unpack.
      final FileUtils.FileCopyResult result = actualBlobPath.endsWith("/")
                                              ? getSegmentFilesFromDirectory(containerName, actualBlobPath, outDir)
                                              : unzipSegmentFiles(containerName, actualBlobPath, outDir);

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

  private FileUtils.FileCopyResult unzipSegmentFiles(
      final String containerName,
      final String blobPath,
      final File outDir
  ) throws IOException
  {
    final ByteSource byteSource = byteSourceFactory.create(containerName, blobPath, azureStorage);
    return CompressionUtils.unzip(
        byteSource,
        outDir,
        AzureUtils.AZURE_RETRY,
        false
    );
  }

  private FileUtils.FileCopyResult getSegmentFilesFromDirectory(
      final String containerName,
      final String blobPathPrefix,
      final File outDir
  ) throws IOException
  {
    final int maxTries = azureAccountConfig.getMaxTries();
    final List<String> blobPaths = azureStorage.listBlobs(containerName, blobPathPrefix, null, maxTries);
    final FileUtils.FileCopyResult copyResult = new FileUtils.FileCopyResult();

    for (final String blobPath : blobPaths) {
      final ByteSource byteSource = byteSourceFactory.create(containerName, blobPath, azureStorage);
      final File outFile = new File(outDir, Paths.get(blobPath).getFileName().toString());
      copyResult.addFiles(
          FileUtils.retryCopy(byteSource, outFile, AzureUtils.AZURE_RETRY, maxTries).getFiles()
      );
    }

    return copyResult;
  }
}
