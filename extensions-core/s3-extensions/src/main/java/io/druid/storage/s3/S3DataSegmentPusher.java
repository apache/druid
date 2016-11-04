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

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;

import io.druid.java.util.common.CompressionUtils;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

public class S3DataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3DataSegmentPusher.class);

  private final RestS3Service s3Client;
  private final S3DataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3DataSegmentPusher(
      RestS3Service s3Client,
      S3DataSegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.s3Client = s3Client;
    this.config = config;
    this.jsonMapper = jsonMapper;

    log.info("Configured S3 as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    return String.format("s3n://%s/%s", config.getBucket(), config.getBaseKey());
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    final String s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), inSegment);

    log.info("Copying segment[%s] to S3 at location[%s]", inSegment.getIdentifier(), s3Path);

    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

    try {
      return S3Utils.retryS3Operation(
          new Callable<DataSegment>()
          {
            @Override
            public DataSegment call() throws Exception
            {
              S3Object toPush = new S3Object(zipOutFile);

              final String outputBucket = config.getBucket();
              final String s3DescriptorPath = S3Utils.descriptorPathForSegmentPath(s3Path);

              toPush.setBucketName(outputBucket);
              toPush.setKey(s3Path);
              if (!config.getDisableAcl()) {
                toPush.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
              }

              log.info("Pushing %s.", toPush);
              s3Client.putObject(outputBucket, toPush);

              final DataSegment outSegment = inSegment.withSize(indexSize)
                                                      .withLoadSpec(
                                                          ImmutableMap.<String, Object>of(
                                                              "type",
                                                              "s3_zip",
                                                              "bucket",
                                                              outputBucket,
                                                              "key",
                                                              toPush.getKey()
                                                          )
                                                      )
                                                      .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

              File descriptorFile = File.createTempFile("druid", "descriptor.json");
              Files.copy(ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(inSegment)), descriptorFile);
              S3Object descriptorObject = new S3Object(descriptorFile);
              descriptorObject.setBucketName(outputBucket);
              descriptorObject.setKey(s3DescriptorPath);
              if (!config.getDisableAcl()) {
                descriptorObject.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
              }

              log.info("Pushing %s", descriptorObject);
              s3Client.putObject(outputBucket, descriptorObject);

              log.info("Deleting zipped index File[%s]", zipOutFile);
              zipOutFile.delete();

              log.info("Deleting descriptor file[%s]", descriptorFile);
              descriptorFile.delete();

              return outSegment;
            }
          }
      );
    }
    catch (ServiceException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
