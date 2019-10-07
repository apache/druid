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
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;

public class AzureDataSegmentPuller
{
  private static final Logger log = new Logger(AzureDataSegmentPuller.class);

  // The azure storage hadoop access pattern is:
  // wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>
  // (from https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage)
  static final String AZURE_STORAGE_HADOOP_PROTOCOL = "wasbs";
  
  static final String AZURE_STORAGE_HOST_ADDRESS = "blob.core.windows.net";

  private final AzureStorage azureStorage;

  @Inject
  public AzureDataSegmentPuller(
      AzureStorage azureStorage
  )
  {
    this.azureStorage = azureStorage;
  }

  org.apache.druid.java.util.common.FileUtils.FileCopyResult getSegmentFiles(
      final String containerName,
      final String blobPath,
      final File outDir
  )
      throws SegmentLoadingException
  {
    try {
      FileUtils.forceMkdir(outDir);

      log.info(
          "Loading container: [%s], with blobPath: [%s] and outDir: [%s]", containerName, blobPath, outDir
      );

      boolean blobPathIsHadoop = blobPath.contains(AZURE_STORAGE_HOST_ADDRESS);
      final String actualBlobPath;
      if (blobPathIsHadoop) {
        // Remove azure's hadoop prefix to match realtime ingestion path
        actualBlobPath = blobPath.substring(
            blobPath.indexOf(AZURE_STORAGE_HOST_ADDRESS) + AZURE_STORAGE_HOST_ADDRESS.length() + 1);
      } else {
        actualBlobPath = blobPath;
      }

      final ByteSource byteSource = new AzureByteSource(azureStorage, containerName, actualBlobPath);
      final org.apache.druid.java.util.common.FileUtils.FileCopyResult result = CompressionUtils.unzip(
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
