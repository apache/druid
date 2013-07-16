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

import com.google.common.base.Throwables;
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
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

/**
 */
public class S3DataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(S3DataSegmentPuller.class);

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;

  @Inject
  public S3DataSegmentPuller(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException
  {
    final S3Coords s3Coords = new S3Coords(segment);

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

    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              long startTime = System.currentTimeMillis();
              S3Object s3Obj = null;

              try {
                s3Obj = s3Client.getObject(s3Coords.bucket, s3Coords.path);

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
                  return null;
                }
                catch (IOException e) {
                  FileUtils.deleteDirectory(outDir);
                  throw new IOException(String.format("Problem decompressing object[%s]", s3Obj), e);
                }
                finally {
                  Closeables.closeQuietly(in);
                }
              }
              finally {
                S3Utils.closeStreamsQuietly(s3Obj);
              }
            }
          }
      );
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  private String toFilename(String key, final String suffix)
  {
    String filename = key.substring(key.lastIndexOf("/") + 1); // characters after last '/'
    filename = filename.substring(0, filename.length() - suffix.length()); // remove the suffix from the end
    return filename;
  }

  private boolean isObjectInBucket(final S3Coords coords) throws SegmentLoadingException
  {
    try {
      return S3Utils.retryS3Operation(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return s3Client.isObjectInBucket(coords.bucket, coords.path);
            }
          }
      );
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "S3 fail! Key[%s]", coords);
    }
  }

  @Override
  public long getLastModified(DataSegment segment) throws SegmentLoadingException
  {
    final S3Coords coords = new S3Coords(segment);
    try {
      final StorageObject objDetails = S3Utils.retryS3Operation(
          new Callable<StorageObject>()
          {
            @Override
            public StorageObject call() throws Exception
            {
              return s3Client.getObjectDetails(coords.bucket, coords.path);
            }
          }
      );
      return objDetails.getLastModifiedDate().getTime();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch (ServiceException e) {
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
