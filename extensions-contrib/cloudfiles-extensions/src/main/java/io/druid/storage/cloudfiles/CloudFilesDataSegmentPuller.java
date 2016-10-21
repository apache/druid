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

package io.druid.storage.cloudfiles;

import com.google.inject.Inject;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CloudFilesDataSegmentPuller implements DataSegmentPuller
{

  private static final Logger log = new Logger(CloudFilesDataSegmentPuller.class);
  private final CloudFilesApi cloudFilesApi;

  @Inject
  public CloudFilesDataSegmentPuller(final CloudFilesApi cloudFilesApi)
  {
    this.cloudFilesApi = cloudFilesApi;
  }

  @Override
  public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final String region = MapUtils.getString(loadSpec, "region");
    final String container = MapUtils.getString(loadSpec, "container");
    final String path = MapUtils.getString(loadSpec, "path");

    log.info("Pulling index at path[%s] to outDir[%s]", path, outDir);
    prepareOutDir(outDir);
    getSegmentFiles(region, container, path, outDir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(String region, String container, String path, File outDir)
      throws SegmentLoadingException
  {
    CloudFilesObjectApiProxy objectApi = new CloudFilesObjectApiProxy(cloudFilesApi, region, container);
    final CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);

    try {
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource, outDir,
          CloudFilesUtils.CLOUDFILESRETRY, true
      );
      log.info("Loaded %d bytes from [%s] to [%s]", result.size(), path, outDir.getAbsolutePath());
      return result;
    }
    catch (Exception e) {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe, "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(), path
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
    finally {
      try {
        byteSource.closeStream();
      }
      catch (IOException ioe) {
        log.warn(ioe, "Failed to close payload for segmente pulled from [%s]", path);
      }
    }
  }

  private void prepareOutDir(final File outDir) throws ISE
  {
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }
  }

}
