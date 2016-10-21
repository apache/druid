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

package io.druid.storage.hdfs;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.URIDataPuller;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.tools.FileObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 */
public class HdfsDataSegmentPuller implements DataSegmentPuller, URIDataPuller
{
  public static final int DEFAULT_RETRY_COUNT = 3;

  /**
   * FileObject.getLastModified and FileObject.delete don't throw IOException. This allows us to wrap those calls
   */
  public static class HdfsIOException extends RuntimeException
  {
    private final IOException cause;

    public HdfsIOException(IOException ex)
    {
      super(ex);
      this.cause = ex;
    }

    protected IOException getIOException()
    {
      return cause;
    }
  }


  public static FileObject buildFileObject(final URI uri, final Configuration config)
  {
    return buildFileObject(uri, config, false);
  }

  public static FileObject buildFileObject(final URI uri, final Configuration config, final Boolean overwrite)
  {
    return new FileObject()
    {
      final Path path = new Path(uri);

      @Override
      public URI toUri()
      {
        return uri;
      }

      @Override
      public String getName()
      {
        return path.getName();
      }

      @Override
      public InputStream openInputStream() throws IOException
      {
        final FileSystem fs = path.getFileSystem(config);
        return fs.open(path);
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        final FileSystem fs = path.getFileSystem(config);
        return fs.create(path, overwrite);
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("HDFS Reader not supported");
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("HDFS CharSequence not supported");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        throw new UOE("HDFS Writer not supported");
      }

      @Override
      public long getLastModified()
      {
        try {
          final FileSystem fs = path.getFileSystem(config);
          return fs.getFileStatus(path).getModificationTime();
        }
        catch (IOException ex) {
          throw new HdfsIOException(ex);
        }
      }

      @Override
      public boolean delete()
      {
        try {
          final FileSystem fs = path.getFileSystem(config);
          return fs.delete(path, false);
        }
        catch (IOException ex) {
          throw new HdfsIOException(ex);
        }
      }
    };
  }

  private static final Logger log = new Logger(HdfsDataSegmentPuller.class);
  protected final Configuration config;

  @Inject
  public HdfsDataSegmentPuller(final Configuration config)
  {
    this.config = config;
  }


  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    getSegmentFiles(getPath(segment), dir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(final Path path, final File outDir) throws SegmentLoadingException
  {
    final LocalFileSystem localFileSystem = new LocalFileSystem();
    try {
      final FileSystem fs = path.getFileSystem(config);
      if (fs.isDirectory(path)) {

        // --------    directory     ---------

        try {
          return RetryUtils.retry(
              new Callable<FileUtils.FileCopyResult>()
              {
                @Override
                public FileUtils.FileCopyResult call() throws Exception
                {
                  if (!fs.exists(path)) {
                    throw new SegmentLoadingException("No files found at [%s]", path.toString());
                  }

                  final RemoteIterator<LocatedFileStatus> children = fs.listFiles(path, false);
                  final ArrayList<FileUtils.FileCopyResult> localChildren = new ArrayList<>();
                  final FileUtils.FileCopyResult result = new FileUtils.FileCopyResult();
                  while (children.hasNext()) {
                    final LocatedFileStatus child = children.next();
                    final Path childPath = child.getPath();
                    final String fname = childPath.getName();
                    if (fs.isDirectory(childPath)) {
                      log.warn("[%s] is a child directory, skipping", childPath.toString());
                    } else {
                      final File outFile = new File(outDir, fname);

                      // Actual copy
                      fs.copyToLocalFile(childPath, new Path(outFile.toURI()));
                      result.addFile(outFile);
                    }
                  }
                  log.info(
                      "Copied %d bytes from [%s] to [%s]",
                      result.size(),
                      path.toString(),
                      outDir.getAbsolutePath()
                  );
                  return result;
                }

              },
              shouldRetryPredicate(),
              DEFAULT_RETRY_COUNT
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      } else if (CompressionUtils.isZip(path.getName())) {

        // --------    zip     ---------

        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            new ByteSource()
            {
              @Override
              public InputStream openStream() throws IOException
              {
                return getInputStream(path);
              }
            }, outDir, shouldRetryPredicate(), false
        );

        log.info(
            "Unzipped %d bytes from [%s] to [%s]",
            result.size(),
            path.toString(),
            outDir.getAbsolutePath()
        );

        return result;
      } else if (CompressionUtils.isGz(path.getName())) {

        // --------    gzip     ---------

        final String fname = path.getName();
        final File outFile = new File(outDir, CompressionUtils.getGzBaseName(fname));
        final FileUtils.FileCopyResult result = CompressionUtils.gunzip(
            new ByteSource()
            {
              @Override
              public InputStream openStream() throws IOException
              {
                return getInputStream(path);
              }
            },
            outFile
        );

        log.info(
            "Gunzipped %d bytes from [%s] to [%s]",
            result.size(),
            path.toString(),
            outFile.getAbsolutePath()
        );
        return result;
      } else {
        throw new SegmentLoadingException("Do not know how to handle file type at [%s]", path.toString());
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Error loading [%s]", path.toString());
    }
  }

  public FileUtils.FileCopyResult getSegmentFiles(URI uri, File outDir) throws SegmentLoadingException
  {
    if (!uri.getScheme().equalsIgnoreCase(HdfsStorageDruidModule.SCHEME)) {
      throw new SegmentLoadingException("Don't know how to load SCHEME for URI [%s]", uri.toString());
    }
    return getSegmentFiles(new Path(uri), outDir);
  }

  public InputStream getInputStream(Path path) throws IOException
  {
    return buildFileObject(path.toUri(), config).openInputStream();
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    if (!uri.getScheme().equalsIgnoreCase(HdfsStorageDruidModule.SCHEME)) {
      throw new IAE("Don't know how to load SCHEME [%s] for URI [%s]", uri.getScheme(), uri.toString());
    }
    return buildFileObject(uri, config).openInputStream();
  }

  /**
   * Return the "version" (aka last modified timestamp) of the URI
   *
   * @param uri The URI of interest
   *
   * @return The last modified timestamp of the uri in String format
   *
   * @throws IOException
   */
  @Override
  public String getVersion(URI uri) throws IOException
  {
    try {
      return String.format("%d", buildFileObject(uri, config).getLastModified());
    }
    catch (HdfsIOException ex) {
      throw ex.getIOException();
    }
  }

  @Override
  public Predicate<Throwable> shouldRetryPredicate()
  {
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable input)
      {
        if (input == null) {
          return false;
        }
        if (input instanceof HdfsIOException) {
          return true;
        }
        if (input instanceof IOException) {
          return true;
        }
        return apply(input.getCause());
      }
    };
  }

  private Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get("path")));
  }
}
