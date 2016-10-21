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
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 */
public class
StreamUtils
{
  // The default buffer size to use (from IOUtils)
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

  /**
   * Copy from an input stream to a file (and buffer it) and close the input stream.
   * <p/>
   * It is highly recommended to use FileUtils.retryCopy whenever possible, and not use a raw `InputStream`
   *
   * @param is   The input stream to copy bytes from. `is` is closed regardless of the copy result.
   * @param file The file to copy bytes to. Any parent directories are automatically created.
   *
   * @return The count of bytes written to the file
   *
   * @throws IOException
   */
  public static long copyToFileAndClose(InputStream is, File file) throws IOException
  {
    file.getParentFile().mkdirs();
    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
      final long result = ByteStreams.copy(is, os);
      // Workarround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
      os.flush();
      return result;
    }
    finally {
      is.close();
    }
  }

  /**
   * Copy bytes from `is` to `file` but timeout if the copy takes too long. The timeout is best effort and not
   * guaranteed. Specifically, `is.read` will not be interrupted.
   *
   * @param is      The `InputStream` to copy bytes from. It is closed regardless of copy results.
   * @param file    The `File` to copy bytes to
   * @param timeout The timeout (in ms) of the copy.
   *
   * @return The size of bytes written to `file`
   *
   * @throws IOException
   * @throws TimeoutException If `timeout` is exceeded
   */
  public static long copyToFileAndClose(InputStream is, File file, long timeout) throws IOException, TimeoutException
  {
    file.getParentFile().mkdirs();
    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
      final long retval = copyWithTimeout(is, os, timeout);
      // Workarround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
      os.flush();
      return retval;
    }
    finally {
      is.close();
    }
  }

  /**
   * Copy from `is` to `os` and close the streams regardless of the result.
   *
   * @param is The `InputStream` to copy results from. It is closed
   * @param os The `OutputStream` to copy results to. It is closed
   *
   * @return The count of bytes written to `os`
   *
   * @throws IOException
   */
  public static long copyAndClose(InputStream is, OutputStream os) throws IOException
  {
    try {
      final long retval = ByteStreams.copy(is, os);
      // Workarround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
      os.flush();
      return retval;
    }
    finally {
      is.close();
      os.close();
    }
  }

  /**
   * Copy from the input stream to the output stream and tries to exit if the copy exceeds the timeout. The timeout
   * is best effort. Specifically, `is.read` will not be interrupted.
   *
   * @param is      The input stream to read bytes from.
   * @param os      The output stream to write bytes to.
   * @param timeout The timeout (in ms) for the copy operation
   *
   * @return The total size of bytes written to `os`
   *
   * @throws IOException
   * @throws TimeoutException If `tiemout` is exceeded
   */
  public static long copyWithTimeout(InputStream is, OutputStream os, long timeout) throws IOException, TimeoutException
  {
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int n;
    long startTime = System.currentTimeMillis();
    long size = 0;
    while (-1 != (n = is.read(buffer))) {
      if (System.currentTimeMillis() - startTime > timeout) {
        throw new TimeoutException(String.format("Copy time has exceeded %,d millis", timeout));
      }
      os.write(buffer, 0, n);
      size += n;
    }
    return size;
  }

  /**
   * Retry copy attempts from input stream to output stream. Does *not* check to make sure data was intact during the transfer
   *
   * @param byteSource  Supplier for input streams to copy from. The stream is closed on every retry.
   * @param byteSink    Supplier for output streams. The stream is closed on every retry.
   * @param shouldRetry Predicate to determine if the throwable is recoverable for a retry
   * @param maxAttempts Maximum number of retries before failing
   */
  public static long retryCopy(
      final ByteSource byteSource,
      final ByteSink byteSink,
      final Predicate<Throwable> shouldRetry,
      final int maxAttempts
  )
  {
    try {
      return RetryUtils.retry(
          new Callable<Long>()
          {
            @Override
            public Long call() throws Exception
            {
              try (InputStream inputStream = byteSource.openStream()) {
                try (OutputStream outputStream = byteSink.openStream()) {
                  final long retval = ByteStreams.copy(inputStream, outputStream);
                  // Workarround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
                  outputStream.flush();
                  return retval;
                }
              }
            }
          },
          shouldRetry,
          maxAttempts
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
