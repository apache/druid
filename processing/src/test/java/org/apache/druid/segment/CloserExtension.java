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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

import java.io.Closeable;
import java.io.IOException;

/**
 * JUnit 5 extension that closes resources registered with {@link #closeLater(Closeable)} after
 * each test.
 */
public class CloserExtension implements BeforeEachCallback, AfterEachCallback, TestExecutionExceptionHandler
{
  private static final Logger LOG = new Logger(CloserExtension.class);

  private final boolean throwException;
  private Closer closer;

  public CloserExtension()
  {
    this(false);
  }

  public CloserExtension(final boolean throwException)
  {
    this.throwException = throwException;
  }

  @Override
  public void beforeEach(final ExtensionContext context)
  {
    if (closer == null) {
      closer = Closer.create();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) throws IOException
  {
    if (closer != null) {
      try {
        closer.close();
      }
      finally {
        closer = null;
      }
    }
  }

  @Override
  public void handleTestExecutionException(final ExtensionContext context, final Throwable throwable) throws Throwable
  {
    if (closer == null) {
      throw throwable;
    }
    throw closer.rethrow(throwable);
  }

  public <T extends Closeable> T closeLater(final T closeable)
  {
    if (closer == null) {
      closer = Closer.create();
    }

    closer.register(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            if (throwException) {
              closeable.close();
            } else {
              try {
                closeable.close();
              }
              catch (IOException e) {
                LOG.warn(e, "Error closing [%s]", closeable);
              }
            }
          }
        }
    );
    return closeable;
  }
}
