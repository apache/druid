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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;

/**
 */
public class StreamUtils
{

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
