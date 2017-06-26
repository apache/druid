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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.Callable;

public class CloudFilesDataSegmentPusher implements DataSegmentPusher
{

  private static final Logger log = new Logger(CloudFilesDataSegmentPusher.class);
  private final CloudFilesObjectApiProxy objectApi;
  private final CloudFilesDataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public CloudFilesDataSegmentPusher(
      final CloudFilesApi cloudFilesApi,
      final CloudFilesDataSegmentPusherConfig config, final ObjectMapper jsonMapper
  )
  {
    this.config = config;
    String region = this.config.getRegion();
    String container = this.config.getContainer();
    this.objectApi = new CloudFilesObjectApiProxy(cloudFilesApi, region, container);
    this.jsonMapper = jsonMapper;

    log.info("Configured CloudFiles as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    return null;
  }

  @Deprecated
  @Override
  public String getPathForHadoop(final String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    final String segmentPath = CloudFilesUtils.buildCloudFilesPath(this.config.getBasePath(), getStorageDir(inSegment));

    File descriptorFile = null;
    File zipOutFile = null;

    try {
      final File descFile = descriptorFile = File.createTempFile("descriptor", ".json");
      final File outFile = zipOutFile = File.createTempFile("druid", "index.zip");

      final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

      log.info("Copying segment[%s] to CloudFiles at location[%s]", inSegment.getIdentifier(), segmentPath);
      return CloudFilesUtils.retryCloudFilesOperation(
          new Callable<DataSegment>()
          {
            @Override
            public DataSegment call() throws Exception
            {
              CloudFilesObject segmentData = new CloudFilesObject(
                  segmentPath, outFile, objectApi.getRegion(),
                  objectApi.getContainer()
              );
              log.info("Pushing %s.", segmentData.getPath());
              objectApi.put(segmentData);

              // Avoid using Guava in DataSegmentPushers because they might be used with very diverse Guava versions in
              // runtime, and because Guava deletes methods over time, that causes incompatibilities.
              Files.write(descFile.toPath(), jsonMapper.writeValueAsBytes(inSegment));
              CloudFilesObject descriptorData = new CloudFilesObject(
                  segmentPath, descFile,
                  objectApi.getRegion(), objectApi.getContainer()
              );
              log.info("Pushing %s.", descriptorData.getPath());
              objectApi.put(descriptorData);

              final DataSegment outSegment = inSegment
                  .withSize(indexSize)
                  .withLoadSpec(makeLoadSpec(new URI(segmentData.getPath())))
                  .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

              return outSegment;
            }
          }, this.config.getOperationMaxRetries()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      if (zipOutFile != null) {
        log.info("Deleting zipped index File[%s]", zipOutFile);
        zipOutFile.delete();
      }

      if (descriptorFile != null) {
        log.info("Deleting descriptor file[%s]", descriptorFile);
        descriptorFile.delete();
      }
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI uri)
  {
    return ImmutableMap.<String, Object>of(
        "type",
        CloudFilesStorageDruidModule.SCHEME,
        "region",
        objectApi.getRegion(),
        "container",
        objectApi.getContainer(),
        "path",
        uri.toString()
    );
  }
}
