/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.URIDataPuller;
import org.apache.druid.utils.CompressionUtils;

import javax.tools.FileObject;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;

/**
 * A data segment puller that also hanldes URI data pulls.
 */
public class S3DataSegmentPuller implements URIDataPuller
{
  public static final int DEFAULT_RETRY_COUNT = 3;

  public static final String SCHEME = S3StorageDruidModule.SCHEME;

  private static final Logger log = new Logger(S3DataSegmentPuller.class);

  protected static final String BUCKET = "bucket";
  protected static final String KEY = "key";

  protected final ServerSideEncryptingAmazonS3 s3Client;

  @Inject
  public S3DataSegmentPuller(ServerSideEncryptingAmazonS3 s3Client)
  {
    this.s3Client = s3Client;
  }

  FileUtils.FileCopyResult getSegmentFiles(final S3Coords s3Coords, final File outDir) throws SegmentLoadingException
  {

    log.info("Pulling index at path[%s] to outDir[%s]", s3Coords, outDir);

    if (!isObjectInBucket(s3Coords)) {
      throw new SegmentLoadingException("IndexFile[%s] does not exist.", s3Coords);
    }

    try {
      org.apache.commons.io.FileUtils.forceMkdir(outDir);

      final URI uri = URI.create(StringUtils.format("s3://%s/%s", s3Coords.bucket, s3Coords.path));
      final ByteSource byteSource = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          try {
            return buildFileObject(uri).openInputStream();
          }
          catch (AmazonServiceException e) {
            if (e.getCause() != null) {
              if (S3Utils.S3RETRY.apply(e)) {
                throw new IOException("Recoverable exception", e);
              }
            }
            throw new RuntimeException(e);
          }
        }
      };
      if (CompressionUtils.isZip(s3Coords.path)) {
        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            byteSource,
            outDir,
            S3Utils.S3RETRY,
            false
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
    if (uri.getScheme().equalsIgnoreCase(SCHEME)) {
      uri = URI.create("s3" + uri.toString().substring(SCHEME.length()));
    } else if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IAE("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    return uri;
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    try {
      return buildFileObject(uri).openInputStream();
    }
    catch (AmazonServiceException e) {
      throw new IOE(e, "Could not load URI [%s]", uri);
    }
  }

  private FileObject buildFileObject(final URI uri) throws AmazonServiceException
  {
    final S3Coords coords = new S3Coords(checkURI(uri));
    final S3ObjectSummary objectSummary = S3Utils.getSingleObjectSummary(s3Client, coords.bucket, coords.path);
    final String path = uri.getPath();

    return new FileObject()
    {
      S3Object s3Object = null;

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

      /**
       * Returns an input stream for a s3 object. The returned input stream is not thread-safe.
       */
      @Override
      public InputStream openInputStream() throws IOException
      {
        try {
          if (s3Object == null) {
            // lazily promote to full GET
            s3Object = s3Client.getObject(objectSummary.getBucketName(), objectSummary.getKey());
          }

          final InputStream in = s3Object.getObjectContent();
          final Closer closer = Closer.create();
          closer.register(in);
          closer.register(s3Object);

          return new FilterInputStream(in)
          {
            @Override
            public void close() throws IOException
            {
              closer.close();
            }
          };
        }
        catch (AmazonServiceException e) {
          throw new IOE(e, "Could not load S3 URI [%s]", uri);
        }
      }

      @Override
      public OutputStream openOutputStream()
      {
        throw new UOE("Cannot stream S3 output");
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors)
      {
        throw new UOE("Cannot open reader");
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors)
      {
        throw new UOE("Cannot open character sequence");
      }

      @Override
      public Writer openWriter()
      {
        throw new UOE("Cannot open writer");
      }

      @Override
      public long getLastModified()
      {
        return objectSummary.getLastModified().getTime();
      }

      @Override
      public boolean delete()
      {
        throw new UOE("Cannot delete S3 items anonymously. jetS3t doesn't support authenticated deletes easily.");
      }
    };
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
        if (e instanceof AmazonServiceException) {
          return S3Utils.isServiceExceptionRecoverable((AmazonServiceException) e);
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
      final S3Coords coords = new S3Coords(checkURI(uri));
      final S3ObjectSummary objectSummary = S3Utils.getSingleObjectSummary(s3Client, coords.bucket, coords.path);
      return StringUtils.format("%d", objectSummary.getLastModified().getTime());
    }
    catch (AmazonServiceException e) {
      if (S3Utils.isServiceExceptionRecoverable(e)) {
        // The recoverable logic is always true for IOException, so we want to only pass IOException if it is recoverable
        throw new IOE(e, "Could not fetch last modified timestamp from URI [%s]", uri);
      } else {
        throw new RE(e, "Error fetching last modified timestamp from URI [%s]", uri);
      }
    }
  }

  private boolean isObjectInBucket(final S3Coords coords) throws SegmentLoadingException
  {
    try {
      return S3Utils.retryS3Operation(
          () -> S3Utils.isObjectInBucketIgnoringPermission(s3Client, coords.bucket, coords.path)
      );
    }
    catch (AmazonS3Exception | IOException e) {
      throw new SegmentLoadingException(e, "S3 fail! Key[%s]", coords);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
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

    public S3Coords(String bucket, String key)
    {
      this.bucket = bucket;
      this.path = key;
    }

    @Override
    public String toString()
    {
      return StringUtils.format("s3://%s/%s", bucket, path);
    }
  }
}
