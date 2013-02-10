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

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.common.s3.S3Utils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.joda.time.DateTime;

import java.io.File;
import java.util.Map;

/**
 */
public class S3SegmentPuller implements SegmentPuller
{
  private static final Logger log = new Logger(S3SegmentPuller.class);
  private static final long DEFAULT_TIMEOUT = 5 * 60 * 1000;

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;

  @Inject
  public S3SegmentPuller(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws StorageAdapterLoadingException
  {
    S3Coords s3Coords = new S3Coords(segment);

    log.info("Loading index at path[%s]", s3Coords);

    if(!isObjectInBucket(s3Coords)){
      throw new StorageAdapterLoadingException("IndexFile[%s] does not exist.", s3Coords);
    }

    long currTime = System.currentTimeMillis();
    File tmpFile = null;
    S3Object s3Obj = null;

    try {
      s3Obj = s3Client.getObject(new S3Bucket(s3Coords.bucket), s3Coords.path);
      tmpFile = File.createTempFile(s3Coords.bucket, new DateTime().toString() + s3Coords.path.replace('/', '_'));
      log.info("Downloading file[%s] to local tmpFile[%s] for segment[%s]", s3Coords, tmpFile, segment);

      StreamUtils.copyToFileAndClose(s3Obj.getDataInputStream(), tmpFile, DEFAULT_TIMEOUT);
      final long downloadEndTime = System.currentTimeMillis();
      log.info("Download of file[%s] completed in %,d millis", tmpFile, downloadEndTime - currTime);

      return tmpFile;
    }
    catch (Exception e) {
      if(tmpFile!=null && tmpFile.exists()){
        tmpFile.delete();
      }
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
    finally {
      S3Utils.closeStreamsQuietly(s3Obj);
    }
  }

  private boolean isObjectInBucket(S3Coords coords) throws StorageAdapterLoadingException {
    try {
      return s3Client.isObjectInBucket(coords.bucket, coords.path);
    } catch (ServiceException e) {
      throw new StorageAdapterLoadingException(e, "Problem communicating with S3 checking bucket/path[%s]", coords);
    }
  }

  @Override
  public long getLastModified(DataSegment segment) throws StorageAdapterLoadingException {
    S3Coords coords = new S3Coords(segment);
    try {
      S3Object objDetails = s3Client.getObjectDetails(new S3Bucket(coords.bucket), coords.path);
      return objDetails.getLastModifiedDate().getTime();
    } catch (S3ServiceException e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
  }

  private class S3Coords {
    String bucket;
    String path;

    public S3Coords(DataSegment segment) {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      bucket = MapUtils.getString(loadSpec, BUCKET);
      path = MapUtils.getString(loadSpec, KEY);
      if(path.startsWith("/")){
        path = path.substring(1);
      }
    }
    public String toString(){
      return String.format("s3://%s/%s", bucket, path);
    }
  }
}
