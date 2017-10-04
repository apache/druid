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
package io.druid.java.util.common;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;

public class FileUtils
{
  /**
   * Useful for retry functionality that doesn't want to stop Throwables, but does want to retry on Exceptions
   */
  public static final Predicate<Throwable> IS_EXCEPTION = new Predicate<Throwable>()
  {
    @Override
    public boolean apply(Throwable input)
    {
      return input instanceof Exception;
    }
  };
  /**
   * Copy input byte source to outFile. If outFile exists, it is attempted to be deleted.
   *
   * @param byteSource  Supplier for an input stream that is to be copied. The resulting stream is closed each iteration
   * @param outFile     Where the file should be written to.
   * @param shouldRetry Predicate indicating if an error is recoverable and should be retried.
   * @param maxAttempts The maximum number of assumed recoverable attempts to try before completely failing.
   *
   * @throws RuntimeException wrapping the inner exception on failure.
   */
  public static FileCopyResult retryCopy(
      final ByteSource byteSource,
      final File outFile,
      final Predicate<Throwable> shouldRetry,
      final int maxAttempts
  )
  {
    try {
      StreamUtils.retryCopy(
          byteSource,
          Files.asByteSink(outFile),
          shouldRetry,
          maxAttempts
      );
      return new FileCopyResult(outFile);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Keeps results of a file copy, including children and total size of the resultant files.
   * This class is NOT thread safe.
   * Child size is eagerly calculated and any modifications to the file after the child is added are not accounted for.
   * As such, this result should be considered immutable, even though it has no way to force that property on the files.
   */
  public static class FileCopyResult
  {
    private final Collection<File> files = Lists.newArrayList();
    private long size = 0L;

    public Collection<File> getFiles()
    {
      return ImmutableList.copyOf(files);
    }

    // Only works for immutable children contents
    public long size()
    {
      return size;
    }

    public FileCopyResult(File... files)
    {
      this(files == null ? ImmutableList.<File>of() : Arrays.asList(files));
    }

    public FileCopyResult(Collection<File> files)
    {
      this.addSizedFiles(files);
    }

    protected void addSizedFiles(Collection<File> files)
    {
      if (files == null || files.isEmpty()) {
        return;
      }
      long size = 0L;
      for (File file : files) {
        size += file.length();
      }
      this.files.addAll(files);
      this.size += size;
    }

    public void addFiles(Collection<File> files)
    {
      this.addSizedFiles(files);
    }

    public void addFile(File file)
    {
      this.addFiles(ImmutableList.of(file));
    }
  }

  /**
   * Fully maps a file read-only in to memory as per
   * {@link FileChannel#map(FileChannel.MapMode, long, long)}.
   *
   * <p>Files are mapped from offset 0 to its length.
   *
   * <p>This only works for files <= {@link Integer#MAX_VALUE} bytes.
   *
   * <p>Similar to {@link Files#map(File)}, but returns {@link MappedByteBufferHandler}, that makes it easier to unmap
   * the buffer within try-with-resources pattern:
   * <pre>{@code
   * try (MappedByteBufferHandler fileMappingHandler = FileUtils.map(file)) {
   *   ByteBuffer fileMapping = fileMappingHandler.get();
   *   // use mapped buffer
   * }}</pre>
   *
   * @param file the file to map
   * @return a {@link MappedByteBufferHandler}, wrapping a read-only buffer reflecting {@code file}
   * @throws FileNotFoundException if the {@code file} does not exist
   * @throws IOException if an I/O error occurs
   *
   * @see FileChannel#map(FileChannel.MapMode, long, long)
   */
  public static MappedByteBufferHandler map(File file) throws IOException
  {
    MappedByteBuffer mappedByteBuffer = Files.map(file);
    return new MappedByteBufferHandler(mappedByteBuffer);
  }
}
