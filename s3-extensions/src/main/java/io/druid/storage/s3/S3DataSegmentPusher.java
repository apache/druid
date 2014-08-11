/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
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
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return String.format("s3n://%s/%s/%s", config.getBucket(), config.getBaseKey(), dataSource);
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    log.info("Uploading [%s] to S3", indexFilesDir);
    final String s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), inSegment);
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
