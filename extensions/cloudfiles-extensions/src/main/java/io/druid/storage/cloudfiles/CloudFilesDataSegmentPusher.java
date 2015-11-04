/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.cloudfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
  public String getPathForHadoop(final String dataSource)
  {
    return null;
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    final String segmentPath = CloudFilesUtils.buildCloudFilesPath(this.config.getBasePath(), inSegment);
    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final File descriptorFile = File.createTempFile("descriptor", ".json");

    log.info("Copying segment[%s] to CloudFiles at location[%s]", inSegment.getIdentifier(), segmentPath);

    try {
      return CloudFilesUtils.retryCloudFilesOperation(
          new Callable<DataSegment>()
          {
            @Override
            public DataSegment call() throws Exception
            {
              CompressionUtils.zip(indexFilesDir, zipOutFile);
              CloudFilesObject segmentData = new CloudFilesObject(
                  segmentPath, zipOutFile, objectApi.getRegion(),
                  objectApi.getContainer()
              );
              log.info("Pushing %s.", segmentData.getPath());
              objectApi.put(segmentData);

              try (FileOutputStream stream = new FileOutputStream(descriptorFile)) {
                stream.write(jsonMapper.writeValueAsBytes(inSegment));
              }
              CloudFilesObject descriptorData = new CloudFilesObject(
                  segmentPath, descriptorFile,
                  objectApi.getRegion(), objectApi.getContainer()
              );
              log.info("Pushing %s.", descriptorData.getPath());
              objectApi.put(descriptorData);

              final DataSegment outSegment = inSegment
                  .withSize(segmentData.getFile().length())
                  .withLoadSpec(
                      ImmutableMap.<String, Object>of(
                          "type",
                          CloudFilesStorageDruidModule.SCHEME,
                          "region",
                          segmentData.getRegion(),
                          "container",
                          segmentData.getContainer(),
                          "path",
                          segmentData.getPath()
                      )
                  )
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
      log.info("Deleting zipped index File[%s]", zipOutFile);
      zipOutFile.delete();

      log.info("Deleting descriptor file[%s]", descriptorFile);
      descriptorFile.delete();
    }
  }
}
