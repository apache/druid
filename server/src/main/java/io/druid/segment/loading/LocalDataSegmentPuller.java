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

package io.druid.segment.loading;

import com.google.common.base.Predicate;
import com.google.common.io.Files;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import javax.tools.FileObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CancellationException;

/**
 */
public class LocalDataSegmentPuller implements DataSegmentPuller, URIDataPuller
{
  public static final int DEFAULT_RETRY_COUNT = 3;

  public static FileObject buildFileObject(final URI uri)
  {
    final Path path = Paths.get(uri);
    final File file = path.toFile();
    return new FileObject()
    {
      @Override
      public URI toUri()
      {
        return uri;
      }

      @Override
      public String getName()
      {
        return path.getFileName().toString();
      }

      @Override
      public InputStream openInputStream() throws IOException
      {
        return new FileInputStream(file);
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        return new FileOutputStream(file);
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        return new FileReader(file);
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("CharSequence not supported");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        return new FileWriter(file);
      }

      @Override
      public long getLastModified()
      {
        return file.lastModified();
      }

      @Override
      public boolean delete()
      {
        return file.delete();
      }
    };
  }

  private static final Logger log = new Logger(LocalDataSegmentPuller.class);

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    getSegmentFiles(getFile(segment), dir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(final File sourceFile, final File dir) throws SegmentLoadingException
  {
    if (sourceFile.isDirectory()) {
      if (sourceFile.equals(dir)) {
        log.info("Asked to load [%s] into itself, done!", dir);
        return new FileUtils.FileCopyResult(sourceFile);
      }

      final File[] files = sourceFile.listFiles();
      if (files == null) {
        throw new SegmentLoadingException("No files found in [%s]", sourceFile.getAbsolutePath());
      }
      final FileUtils.FileCopyResult result = new FileUtils.FileCopyResult(sourceFile);
      for (final File oldFile : files) {
        if (oldFile.isDirectory()) {
          log.info("[%s] is a child directory, skipping", oldFile.getAbsolutePath());
          continue;
        }

        result.addFiles(
            FileUtils.retryCopy(
                Files.asByteSource(oldFile),
                new File(dir, oldFile.getName()),
                shouldRetryPredicate(),
                DEFAULT_RETRY_COUNT
            ).getFiles()
        );
      }
      log.info(
          "Coppied %d bytes from [%s] to [%s]",
          result.size(),
          sourceFile.getAbsolutePath(),
          dir.getAbsolutePath()
      );
      return result;
    }
    if (CompressionUtils.isZip(sourceFile.getName())) {
      try {
        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            Files.asByteSource(sourceFile),
            dir,
            shouldRetryPredicate(),
            false
        );
        log.info(
            "Unzipped %d bytes from [%s] to [%s]",
            result.size(),
            sourceFile.getAbsolutePath(),
            dir.getAbsolutePath()
        );
        return result;
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to unzip file [%s]", sourceFile.getAbsolutePath());
      }
    }
    if (CompressionUtils.isGz(sourceFile.getName())) {
      final File outFile = new File(dir, CompressionUtils.getGzBaseName(sourceFile.getName()));
      final FileUtils.FileCopyResult result = CompressionUtils.gunzip(
          Files.asByteSource(sourceFile),
          outFile,
          shouldRetryPredicate()
      );
      log.info(
          "Gunzipped %d bytes from [%s] to [%s]",
          result.size(),
          sourceFile.getAbsolutePath(),
          outFile.getAbsolutePath()
      );
      return result;
    }
    throw new SegmentLoadingException("Do not know how to handle source [%s]", sourceFile.getAbsolutePath());
  }


  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    return buildFileObject(uri).openInputStream();
  }

  /**
   * Returns the "version" (aka last modified timestamp) of the URI of interest
   *
   * @param uri The URI to check the last modified timestamp
   *
   * @return The last modified timestamp in ms of the URI in String format
   */
  @Override
  public String getVersion(URI uri)
  {
    return String.format("%d", buildFileObject(uri).getLastModified());
  }

  @Override
  public Predicate<Throwable> shouldRetryPredicate()
  {
    // It would be nice if there were better logic for smarter retries. For example: If the error is that the file is
    // not found, there's only so much that retries would do (unless the file was temporarily absent for some reason).
    // Since this is not a commonly used puller in production, and in general is more useful in testing/debugging,
    // I do not have a good sense of what kind of Exceptions people would expect to encounter in the wild
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable input)
      {
        return !(input instanceof InterruptedException)
               && !(input instanceof CancellationException)
               && (input instanceof Exception);
      }
    };
  }

  private File getFile(DataSegment segment) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final File path = new File(MapUtils.getString(loadSpec, "path"));

    if (!path.exists()) {
      throw new SegmentLoadingException("Asked to load path[%s], but it doesn't exist.", path);
    }

    return path;
  }
}
