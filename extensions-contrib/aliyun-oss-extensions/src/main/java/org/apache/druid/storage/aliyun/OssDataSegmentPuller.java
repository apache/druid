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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.druid.data.input.impl.CloudObjectLocation;
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
public class OssDataSegmentPuller implements URIDataPuller
{
  private static final Logger log = new Logger(OssDataSegmentPuller.class);

  static final String BUCKET = "bucket";
  protected static final String KEY = "key";

  protected final OSS client;

  @Inject
  public OssDataSegmentPuller(OSS client)
  {
    this.client = client;
  }

  FileUtils.FileCopyResult getSegmentFiles(final CloudObjectLocation ossCoords, final File outDir)
      throws SegmentLoadingException
  {

    log.info("Pulling index at path[%s] to outDir[%s]", ossCoords, outDir);

    if (!isObjectInBucket(ossCoords)) {
      throw new SegmentLoadingException("IndexFile[%s] does not exist.", ossCoords);
    }

    try {
      org.apache.commons.io.FileUtils.forceMkdir(outDir);

      final URI uri = ossCoords.toUri(OssStorageDruidModule.SCHEME);
      final ByteSource byteSource = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          try {
            return buildFileObject(uri).openInputStream();
          }
          catch (OSSException e) {
            if (e.getCause() != null) {
              if (OssUtils.RETRYABLE.apply(e)) {
                throw new IOException("Recoverable exception", e);
              }
            }
            throw new RuntimeException(e);
          }
        }
      };
      if (CompressionUtils.isZip(ossCoords.getPath())) {
        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            byteSource,
            outDir,
            OssUtils.RETRYABLE,
            false
        );
        log.info("Loaded %d bytes from [%s] to [%s]", result.size(), ossCoords.toString(), outDir.getAbsolutePath());
        return result;
      }
      if (CompressionUtils.isGz(ossCoords.getPath())) {
        final String fname = Files.getNameWithoutExtension(uri.getPath());
        final File outFile = new File(outDir, fname);

        final FileUtils.FileCopyResult result = CompressionUtils.gunzip(byteSource, outFile, OssUtils.RETRYABLE);
        log.info("Loaded %d bytes from [%s] to [%s]", result.size(), ossCoords.toString(), outFile.getAbsolutePath());
        return result;
      }
      throw new IAE("Do not know how to load file type at [%s]", uri.toString());
    }
    catch (Exception e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(),
            ossCoords.toString()
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    try {
      return buildFileObject(uri).openInputStream();
    }
    catch (OSSException e) {
      throw new IOE(e, "Could not load URI [%s]", uri);
    }
  }

  private FileObject buildFileObject(final URI uri) throws OSSException
  {
    final CloudObjectLocation coords = new CloudObjectLocation(OssUtils.checkURI(uri));
    final OSSObjectSummary objectSummary =
        OssUtils.getSingleObjectSummary(client, coords.getBucket(), coords.getPath());
    final String path = uri.getPath();

    return new FileObject()
    {
      OSSObject ossObject = null;

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
       * Returns an input stream for an OSS object. The returned input stream is not thread-safe.
       */
      @Override
      public InputStream openInputStream() throws IOException
      {
        try {
          if (ossObject == null) {
            // lazily promote to full GET
            ossObject = client.getObject(objectSummary.getBucketName(), objectSummary.getKey());
          }

          final InputStream in = ossObject.getObjectContent();
          final Closer closer = Closer.create();
          closer.register(in);
          closer.register(ossObject);

          return new FilterInputStream(in)
          {
            @Override
            public void close() throws IOException
            {
              closer.close();
            }
          };
        }
        catch (OSSException e) {
          throw new IOE(e, "Could not load OSS URI [%s]", uri);
        }
      }

      @Override
      public OutputStream openOutputStream()
      {
        throw new UOE("Cannot stream OSS output");
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
        throw new UOE("Cannot delete OSS items anonymously. jetS3t doesn't support authenticated deletes easily.");
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
        if (e instanceof OSSException) {
          return OssUtils.isServiceExceptionRecoverable((OSSException) e);
        }
        if (OssUtils.RETRYABLE.apply(e)) {
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
   * @return The time in ms of the last modification of the URI in String format
   * @throws IOException
   */
  @Override
  public String getVersion(URI uri) throws IOException
  {
    try {
      final CloudObjectLocation coords = new CloudObjectLocation(OssUtils.checkURI(uri));
      final OSSObjectSummary objectSummary =
          OssUtils.getSingleObjectSummary(client, coords.getBucket(), coords.getPath());
      return StringUtils.format("%d", objectSummary.getLastModified().getTime());
    }
    catch (OSSException e) {
      if (OssUtils.isServiceExceptionRecoverable(e)) {
        // The recoverable logic is always true for IOException, so we want to only pass IOException if it is recoverable
        throw new IOE(e, "Could not fetch last modified timestamp from URI [%s]", uri);
      } else {
        throw new RE(e, "Error fetching last modified timestamp from URI [%s]", uri);
      }
    }
  }

  private boolean isObjectInBucket(final CloudObjectLocation coords) throws SegmentLoadingException
  {
    try {
      return OssUtils.retry(
          () -> OssUtils.isObjectInBucketIgnoringPermission(client, coords.getBucket(), coords.getPath())
      );
    }
    catch (OSSException | IOException e) {
      throw new SegmentLoadingException(e, "fail! Key[%s]", coords);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
