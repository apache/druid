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

package org.apache.druid.data.input.impl.prefetch;

import com.google.common.base.Predicate;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.RetryUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class Fetchers
{
  /**
   * Copies data from the given InputStream to the given file.
   * This method is supposed to be used for copying large files.
   * The output file is deleted automatically if copy fails.
   *
   * @param object              object to open
   * @param objectOpenFunction  function to open the given object
   * @param outFile             file to write data
   * @param fetchBuffer         a buffer to copy data from the input stream to the file
   * @param retryCondition      condition which should be satisfied for retry
   * @param numRetries          max number of retries
   * @param messageOnRetry      log message on retry
   *
   * @return the number of bytes copied
   */
  public static <T> long fetch(
      T object,
      ObjectOpenFunction<T> objectOpenFunction,
      File outFile,
      byte[] fetchBuffer,
      Predicate<Throwable> retryCondition,
      int numRetries,
      String messageOnRetry
  ) throws IOException
  {
    try {
      return RetryUtils.retry(
          () -> {
            try (InputStream inputStream = objectOpenFunction.open(object);
                 OutputStream out = new FileOutputStream(outFile)) {
              return IOUtils.copyLarge(inputStream, out, fetchBuffer);
            }
          },
          retryCondition,
          outFile::delete,
          numRetries,
          messageOnRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  private Fetchers()
  {
  }
}
