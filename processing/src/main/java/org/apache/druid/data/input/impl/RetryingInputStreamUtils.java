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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;

public class RetryingInputStreamUtils
{
  private static final Logger log = new Logger(RetryingInputStreamUtils.class);

  protected static void handleInputStreamOpenError(
      Throwable t,
      Predicate<Throwable> retryCondition,
      int nTry,
      int maxTries,
      long offset,
      boolean doWait
  ) throws IOException
  {
    log.info("Encountered an error while opening the input stream, attempt number: %d, error: %s", nTry, t.getMessage());
    final int nextTry = nTry + 1;
    if (nextTry < maxTries && retryCondition.apply(t)) {
      final String message = StringUtils.format("Stream interrupted at position [%d]", offset);
      try {
        if (doWait) {
          RetryUtils.awaitNextRetry(t, message, nextTry, maxTries, false);
        }
      }
      catch (InterruptedException e) {
        t.addSuppressed(e);
        throwAsIOException(t);
      }
    } else {
      throwAsIOException(t);
    }
  }

  protected static void throwAsIOException(Throwable t) throws IOException
  {
    Throwables.propagateIfInstanceOf(t, IOException.class);
    throw new IOException(t);
  }
}
