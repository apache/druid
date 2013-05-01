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

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.common.s3.S3Utils;
import com.metamx.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 */
public class CassandraDataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(CassandraDataSegmentPuller.class);

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;

  @Inject
  public CassandraDataSegmentPuller(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {
    S3Coords s3Coords = new S3Coords(segment);

    log.info("Pulling index at path[%s] to outDir[%s]", s3Coords, outDir);

    if (!isObjectInBucket(s3Coords)) {
      throw new SegmentLoadingException("IndexFile[%s] does not exist.", s3Coords);
    }

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }

    long startTime = System.currentTimeMillis();
    S3Object s3Obj = null;

    try {
      s3Obj = s3Client.getObject(new S3Bucket(s3Coords.bucket), s3Coords.path);

      InputStream in = null;
      try {
        in = s3Obj.getDataInputStream();
        final String key = s3Obj.getKey();
        if (key.endsWith(".zip")) {
          CompressionUtils.unzip(in, outDir);
        } else if (key.endsWith(".gz")) {
          final File outFile = new File(outDir, toFilename(key, ".gz"));
          ByteStreams.copy(new GZIPInputStream(in), Files.newOutputStreamSupplier(outFile));
        } else {
          ByteStreams.copy(in, Files.newOutputStreamSupplier(new File(outDir, toFilename(key, ""))));
        }
        log.info("Pull of file[%s] completed in %,d millis", s3Obj, System.currentTimeMillis() - startTime);
      }
      catch (IOException e) {
        FileUtils.deleteDirectory(outDir);
        throw new SegmentLoadingException(e, "Problem decompressing object[%s]", s3Obj);
      }
      finally {
        Closeables.closeQuietly(in);
      }
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
    finally {
      S3Utils.closeStreamsQuietly(s3Obj);
    }

  }

  private String toFilename(String key, final String suffix)
  {
    String filename = key.substring(key.lastIndexOf("/") + 1); // characters after last '/'
    filename = filename.substring(0, filename.length() - suffix.length()); // remove the suffix from the end
    return filename;
  }

  private boolean isObjectInBucket(S3Coords coords) throws SegmentLoadingException
  {
    try {
      return s3Client.isObjectInBucket(coords.bucket, coords.path);
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "S3 fail!  Key[%s]", coords);
    }
  }

  @Override
  public long getLastModified(DataSegment segment) throws SegmentLoadingException
  {
    S3Coords coords = new S3Coords(segment);
    try {
      S3Object objDetails = s3Client.getObjectDetails(new S3Bucket(coords.bucket), coords.path);
      return objDetails.getLastModifiedDate().getTime();
    }
    catch (S3ServiceException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  private static class S3Coords
  {
    String bucket;
    String path;

    public S3Coords(DataSegment segment)
    {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      bucket = MapUtils.getString(loadSpec, BUCKET);
      path = MapUtils.getString(loadSpec, KEY);
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
    }

    public String toString()
    {
      return String.format("s3://%s/%s", bucket, path);
    }
  }
}
