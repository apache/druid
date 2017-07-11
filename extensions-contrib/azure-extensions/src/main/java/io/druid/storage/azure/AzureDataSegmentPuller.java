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

package io.druid.storage.azure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AzureDataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(AzureDataSegmentPuller.class);

  private final AzureStorage azureStorage;

  @Inject
  public AzureDataSegmentPuller(
      AzureStorage azureStorage
  )
  {
    this.azureStorage = azureStorage;
  }

  public io.druid.java.util.common.FileUtils.FileCopyResult getSegmentFiles(
      final String containerName,
      final String blobPath,
      final File outDir
  )
      throws SegmentLoadingException
  {
    try {
      prepareOutDir(outDir);

      final ByteSource byteSource = new AzureByteSource(azureStorage, containerName, blobPath);
      final io.druid.java.util.common.FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource,
          outDir,
          AzureUtils.AZURE_RETRY,
          false
      );

      log.info("Loaded %d bytes from [%s] to [%s]", result.size(), blobPath, outDir.getAbsolutePath());
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

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {

    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final String containerName = MapUtils.getString(loadSpec, "containerName");
    final String blobPath = MapUtils.getString(loadSpec, "blobPath");

    getSegmentFiles(containerName, blobPath, outDir);
  }

  @VisibleForTesting
  void prepareOutDir(final File outDir) throws IOException
  {
    FileUtils.forceMkdir(outDir);
  }
}
