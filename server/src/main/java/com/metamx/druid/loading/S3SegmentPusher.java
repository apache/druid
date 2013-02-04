/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.loading;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.metamx.common.ISE;
import com.metamx.common.StreamUtils;
import com.metamx.druid.client.DataSegment;
import com.metamx.emitter.EmittingLogger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class S3SegmentPusher implements SegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3SegmentPusher.class);
  private static final Joiner JOINER = Joiner.on("/").skipNulls();

  private final RestS3Service s3Client;
  private final S3SegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  public S3SegmentPusher(
    RestS3Service s3Client,
    S3SegmentPusherConfig config,
    ObjectMapper jsonMapper
  )
  {
    this.s3Client = s3Client;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public DataSegment push(File file, DataSegment segment) throws IOException
  {
    log.info("Uploading [%s] to S3", file);
    String outputKey = JOINER.join(
        config.getBaseKey().isEmpty() ? null : config.getBaseKey(),
        segment.getDataSource(),
        String.format(
            "%s_%s",
            segment.getInterval().getStart(),
            segment.getInterval().getEnd()
        ),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum()
    );

    File indexFilesDir = file;

    long indexSize = 0;
    final File zipOutFile = File.createTempFile("druid", "index.zip");
    ZipOutputStream zipOut = null;
    try {
      zipOut = new ZipOutputStream(new FileOutputStream(zipOutFile));
      File[] indexFiles = indexFilesDir.listFiles();
      for (File indexFile : indexFiles) {
        log.info("Adding indexFile[%s] with size[%,d].  Total size[%,d]", indexFile, indexFile.length(), indexSize);
        if (indexFile.length() >= Integer.MAX_VALUE) {
            throw new ISE("indexFile[%s] too large [%,d]", indexFile, indexFile.length());
        }
        zipOut.putNextEntry(new ZipEntry(indexFile.getName()));
        IOUtils.copy(new FileInputStream(indexFile), zipOut);
        indexSize += indexFile.length();
      }
    }
    finally {
      Closeables.closeQuietly(zipOut);
    }

    try {
      S3Object toPush = new S3Object(zipOutFile);

      final String outputBucket = config.getBucket();
      toPush.setBucketName(outputBucket);
      toPush.setKey(outputKey + "/index.zip");
      toPush.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);

      log.info("Pushing %s.", toPush);
      s3Client.putObject(outputBucket, toPush);

      DataSegment outputSegment = segment.withSize(indexSize)
                                         .withLoadSpec(
                                             ImmutableMap.<String, Object>of(
                                                 "type", "s3_zip",
                                                 "bucket", outputBucket,
                                                 "key", toPush.getKey()
                                             )
                                         );

      File descriptorFile = File.createTempFile("druid", "descriptor.json");
      StreamUtils.copyToFileAndClose(new ByteArrayInputStream(jsonMapper.writeValueAsBytes(segment)), descriptorFile);
      S3Object descriptorObject = new S3Object(descriptorFile);
      descriptorObject.setBucketName(outputBucket);
      descriptorObject.setKey(outputKey + "/descriptor.json");
      descriptorObject.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);

      log.info("Pushing %s", descriptorObject);
      s3Client.putObject(outputBucket, descriptorObject);

      log.info("Deleting Index File[%s]", indexFilesDir);
      FileUtils.deleteDirectory(indexFilesDir);

      log.info("Deleting zipped index File[%s]", zipOutFile);
      zipOutFile.delete();

      log.info("Deleting descriptor file[%s]", descriptorFile);
      descriptorFile.delete();

      return outputSegment;
    }
    catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
    catch (S3ServiceException e) {
      throw new IOException(e);
    }
  }
}