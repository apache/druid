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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.URIDataPuller;
import io.druid.timeline.DataSegment;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.tools.FileObject;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.StorageObject;

/**
 * A data segment puller that also hanldes URI data pulls.
 */
public class S3DataSegmentPuller implements DataSegmentPuller, URIDataPuller
{
  public static final int DEFAULT_RETRY_COUNT = 3;

  public static FileObject buildFileObject(final URI uri, final RestS3Service s3Client) throws ServiceException
  {
    final S3Coords coords = new S3Coords(checkURI(uri));
    final StorageObject s3Obj = s3Client.getObjectDetails(coords.bucket, coords.path);
    final String path = uri.getPath();

    return new FileObject()
    {
      final Object inputStreamOpener = new Object();
      volatile boolean streamAcquired = false;
      volatile StorageObject storageObject = s3Obj;

      @Override
      public URI toUri()
      {
        return uri;
      }

      @Override
      public String getName()
      {
        final String ext = Files.getFileExtension(path);
        return Files.getNameWithoutExtension(path) + (Strings.isNullOrEmpty(ext) ? "" : ("." + ext));
      }

      @Override
      public InputStream openInputStream() throws IOException
      {
        try {
          synchronized (inputStreamOpener) {
            if (streamAcquired) {
              return storageObject.getDataInputStream();
            }
            // lazily promote to full GET
            storageObject = s3Client.getObject(s3Obj.getBucketName(), s3Obj.getKey());
            final InputStream stream = storageObject.getDataInputStream();
            streamAcquired = true;
            return stream;
          }
        }
        catch (ServiceException e) {
          throw new IOException(StringUtils.safeFormat("Could not load S3 URI [%s]", uri), e);
        }
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        throw new UOE("Cannot stream S3 output");
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("Cannot open reader");
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("Cannot open character sequence");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        throw new UOE("Cannot open writer");
      }

      @Override
      public long getLastModified()
      {
        return s3Obj.getLastModifiedDate().getTime();
      }

      @Override
      public boolean delete()
      {
        throw new UOE("Cannot delete S3 items anonymously. jetS3t doesn't support authenticated deletes easily.");
      }
    };
  }

  public static final String scheme = S3StorageDruidModule.SCHEME;

  private static final Logger log = new Logger(S3DataSegmentPuller.class);

  protected static final String BUCKET = "bucket";
  protected static final String KEY = "key";

  protected final RestS3Service s3Client;

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
    getSegmentFiles(new S3Coords(segment), outDir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(final S3Coords s3Coords, final File outDir)
      throws SegmentLoadingException
  {

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
      final URI uri = URI.create(String.format("s3://%s/%s", s3Coords.bucket, s3Coords.path));
      final ByteSource byteSource = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          try {
            return buildFileObject(uri, s3Client).openInputStream();
          }
          catch (ServiceException e) {
            if (e.getCause() != null) {
              if (S3Utils.S3RETRY.apply(e)) {
                throw new IOException("Recoverable exception", e);
              }
            }
            throw Throwables.propagate(e);
          }
        }
      };
      if (CompressionUtils.isZip(s3Coords.path)) {
        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            byteSource,
            outDir,
            S3Utils.S3RETRY,
            true
        );
        log.info("Loaded %d bytes from [%s] to [%s]", result.size(), s3Coords.toString(), outDir.getAbsolutePath());
        return result;
      }
      if (CompressionUtils.isGz(s3Coords.path)) {
        final String fname = Files.getNameWithoutExtension(uri.getPath());
        final File outFile = new File(outDir, fname);

        final FileUtils.FileCopyResult result = CompressionUtils.gunzip(byteSource, outFile, S3Utils.S3RETRY);
        log.info("Loaded %d bytes from [%s] to [%s]", result.size(), s3Coords.toString(), outFile.getAbsolutePath());
        return result;
      }
      throw new IAE("Do not know how to load file type at [%s]", uri.toString());
    }
    catch (Exception e) {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(),
            s3Coords.toString()
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  public static URI checkURI(URI uri)
  {
    if (uri.getScheme().equalsIgnoreCase(scheme)) {
      uri = URI.create("s3" + uri.toString().substring(scheme.length()));
    } else if (!uri.getScheme().equalsIgnoreCase("s3")) {
      throw new IAE("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    return uri;
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    try {
      return buildFileObject(uri, s3Client).openInputStream();
    }
    catch (ServiceException e) {
      throw new IOException(String.format("Could not load URI [%s]", uri.toString()), e);
    }
  }

  @Override
  public Predicate<Throwable> shouldRetryPredicate()
  {
    // Yay! smart retries!
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        if (e == null) {
          return false;
        }
        if (e instanceof ServiceException) {
          return S3Utils.isServiceExceptionRecoverable((ServiceException) e);
        }
        if (S3Utils.S3RETRY.apply(e)) {
          return true;
        }
        // Look all the way down the cause chain, just in case something wraps it deep.
        return apply(e.getCause());
      }
    };
  }

  /**
   * Returns the "version" (aka last modified timestamp) of the URI
   *
   * @param uri The URI to check the last timestamp
   *
   * @return The time in ms of the last modification of the URI in String format
   *
   * @throws IOException
   */
  @Override
  public String getVersion(URI uri) throws IOException
  {
    try {
      final FileObject object = buildFileObject(uri, s3Client);
      return String.format("%d", object.getLastModified());
    }
    catch (ServiceException e) {
      if (S3Utils.isServiceExceptionRecoverable(e)) {
        // The recoverable logic is always true for IOException, so we want to only pass IOException if it is recoverable
        throw new IOException(
            String.format("Could not fetch last modified timestamp from URI [%s]", uri.toString()),
            e
        );
      } else {
        throw Throwables.propagate(e);
      }
    }
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
              return S3Utils.isObjectInBucket(s3Client, coords.bucket, coords.path);
            }
          }
      );
    }
    catch (S3ServiceException | IOException e) {
      throw new SegmentLoadingException(e, "S3 fail! Key[%s]", coords);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected static class S3Coords
  {
    String bucket;
    String path;

    public S3Coords(URI uri)
    {
      if (!"s3".equalsIgnoreCase(uri.getScheme())) {
        throw new IAE("Unsupported scheme: [%s]", uri.getScheme());
      }
      bucket = uri.getHost();
      String path = uri.getPath();
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
      this.path = path;
    }

    public S3Coords(DataSegment segment)
    {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      bucket = MapUtils.getString(loadSpec, BUCKET);
      path = MapUtils.getString(loadSpec, KEY);
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
    }

    public S3Coords(String bucket, String key)
    {
      this.bucket = bucket;
      this.path = key;
    }

    public String toString()
    {
      return String.format("s3://%s/%s", bucket, path);
    }
  }
}
