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

import com.google.common.io.Closeables;
import com.metamx.common.MapUtils;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.common.s3.S3Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.joda.time.DateTime;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 */
public class S3ZippedSegmentPuller implements SegmentPuller
{
  private static final Logger log = new Logger(S3ZippedSegmentPuller.class);

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;
  private final S3SegmentGetterConfig config;

  public S3ZippedSegmentPuller(
      RestS3Service s3Client,
      S3SegmentGetterConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws StorageAdapterLoadingException
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    String s3Bucket = MapUtils.getString(loadSpec, "bucket");
    String s3Path = MapUtils.getString(loadSpec, "key");

    if (s3Path.startsWith("/")) {
      s3Path = s3Path.substring(1);
    }

    log.info("Loading index at path[s3://%s/%s]", s3Bucket, s3Path);

    S3Object s3Obj = null;
    File tmpFile = null;
    try {
      if (!s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        throw new StorageAdapterLoadingException("IndexFile[s3://%s/%s] does not exist.", s3Bucket, s3Path);
      }

      File cacheFile = new File(config.getCacheDirectory(), computeCacheFilePath(s3Bucket, s3Path));

      if (cacheFile.exists()) {
        S3Object objDetails = s3Client.getObjectDetails(new S3Bucket(s3Bucket), s3Path);
        DateTime cacheFileLastModified = new DateTime(cacheFile.lastModified());
        DateTime s3ObjLastModified = new DateTime(objDetails.getLastModifiedDate().getTime());
        if (cacheFileLastModified.isAfter(s3ObjLastModified)) {
          log.info(
              "Found cacheFile[%s] with modified[%s], which is after s3Obj[%s].  Using.",
              cacheFile,
              cacheFileLastModified,
              s3ObjLastModified
          );
          return cacheFile;
        }
        FileUtils.deleteDirectory(cacheFile);
      }

      long currTime = System.currentTimeMillis();

      tmpFile = File.createTempFile(s3Bucket, new DateTime().toString());
      log.info(
          "Downloading file[s3://%s/%s] to local tmpFile[%s] for cacheFile[%s]",
          s3Bucket, s3Path, tmpFile, cacheFile
      );

      s3Obj = s3Client.getObject(new S3Bucket(s3Bucket), s3Path);
      StreamUtils.copyToFileAndClose(s3Obj.getDataInputStream(), tmpFile);
      final long downloadEndTime = System.currentTimeMillis();
      log.info("Download of file[%s] completed in %,d millis", cacheFile, downloadEndTime - currTime);

      if (cacheFile.exists()) {
        FileUtils.deleteDirectory(cacheFile);
      }
      cacheFile.mkdirs();

      ZipInputStream zipIn = null;
      OutputStream out = null;
      ZipEntry entry = null;
      try {
        zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(tmpFile)));
        while ((entry = zipIn.getNextEntry()) != null) {
          out = new FileOutputStream(new File(cacheFile, entry.getName()));
          IOUtils.copy(zipIn, out);
          zipIn.closeEntry();
          Closeables.closeQuietly(out);
          out = null;
        }
      }
      finally {
        Closeables.closeQuietly(out);
        Closeables.closeQuietly(zipIn);
      }

      long endTime = System.currentTimeMillis();
      log.info("Local processing of file[%s] done in %,d millis", cacheFile, endTime - downloadEndTime);

      log.info("Deleting tmpFile[%s]", tmpFile);
      tmpFile.delete();

      return cacheFile;
    }
    catch (Exception e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
    finally {
      S3Utils.closeStreamsQuietly(s3Obj);
      if (tmpFile != null && tmpFile.exists()) {
        log.warn("Deleting tmpFile[%s] in finally block.  Why?", tmpFile);
        tmpFile.delete();
      }
    }
  }

  private String computeCacheFilePath(String s3Bucket, String s3Path)
  {
    return new File(String.format("%s/%s", s3Bucket, s3Path)).getParent();
  }

  @Override
  public boolean cleanSegmentFiles(DataSegment segment) throws StorageAdapterLoadingException
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    File cacheFile = new File(
        config.getCacheDirectory(),
        computeCacheFilePath(
            MapUtils.getString(loadSpec, BUCKET),
            MapUtils.getString(loadSpec, KEY)
        )
    );

    try {
      log.info("Deleting directory[%s]", cacheFile);
      FileUtils.deleteDirectory(cacheFile);
    }
    catch (IOException e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }

    return true;
  }
}
