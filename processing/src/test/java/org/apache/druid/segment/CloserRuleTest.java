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

import com.google.common.util.concurrent.Runnables;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CloserRuleTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testCloses() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close()
          {
            closed.set(true);
          }
        }
    );
    run(closer, Runnables.doNothing());
    Assert.assertTrue(closed.get());
  }

  @Test
  public void testPreservesException() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close()
          {
            closed.set(true);
          }
        }
    );

    final String msg = "You can't divide by zero, you can only take the limit of such!";
    Exception ex = null;
    try {
      run(
          closer,
          () -> {
            throw new ArithmeticException(msg);
          }
      );
    }
    catch (Exception e) {
      ex = e;
    }
    Assert.assertTrue(closed.get());
    Assert.assertNotNull(ex);
    Assert.assertTrue(ex instanceof ArithmeticException);
    Assert.assertEquals(msg, ex.getMessage());
  }


  @Test
  public void testSuppressed()
  {
    final CloserRule closer = new CloserRule(true);
    final AtomicBoolean closed = new AtomicBoolean(false);
    final String ioExceptionMsg = "You can't triple stamp a double stamp!";
    final IOException suppressed = new IOException(ioExceptionMsg);
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw suppressed;
          }
        }
    );
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close()
          {
            closed.set(true);
          }
        }
    );

    final String msg = "You can't divide by zero, you can only take the limit of such!";
    final ArithmeticException arithmeticException = new ArithmeticException(msg);

    Throwable ex = null;
    try {
      run(
          closer,
          () -> {
            throw arithmeticException;
          }
      );
    }
    catch (Throwable e) {
      ex = e;
    }
    Assert.assertEquals(arithmeticException, ex);
    Assert.assertNotNull(ex);
    Assert.assertNotNull(ex.getSuppressed());
    Assert.assertEquals(suppressed, ex.getSuppressed()[0]);
  }

  @Test
  public void testThrowsCloseException()
  {
    final CloserRule closer = new CloserRule(true);
    final String ioExceptionMsg = "You can't triple stamp a double stamp!";
    final IOException ioException = new IOException(ioExceptionMsg);
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw ioException;
          }
        }
    );
    Throwable ex = null;
    try {
      run(closer, Runnables.doNothing());
    }
    catch (Throwable throwable) {
      ex = throwable;
    }
    Assert.assertEquals(ioException, ex);
  }


  @Test
  public void testJustLogs() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final String ioExceptionMsg = "You can't triple stamp a double stamp!";
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw new IOException(ioExceptionMsg);
          }
        }
    );
    run(closer, Runnables.doNothing());
  }

  @Test
  public void testJustLogsAnything() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final String ioExceptionMsg = "You can't triple stamp a double stamp!";
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw new IOException(ioExceptionMsg);
          }
        }
    );
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw new IOException(ioExceptionMsg);
          }
        }
    );
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw new IOException(ioExceptionMsg);
          }
        }
    );
    run(closer, Runnables.doNothing());
  }

  @Test
  public void testClosesEverything()
  {
    final AtomicLong counter = new AtomicLong(0L);
    final CloserRule closer = new CloserRule(true);
    final String ioExceptionMsg = "You can't triple stamp a double stamp!";
    final List<IOException> ioExceptions = Arrays.asList(
        new IOException(ioExceptionMsg),
        null,
        new IOException(ioExceptionMsg),
        null,
        new IOException(ioExceptionMsg),
        null
    );
    for (final IOException throwable : ioExceptions) {
      closer.closeLater(
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              counter.incrementAndGet();
              if (throwable != null) {
                throw throwable;
              }
            }
          }
      );
    }
    Throwable ex = null;
    try {
      run(closer, Runnables.doNothing());
    }
    catch (Throwable throwable) {
      ex = throwable;
    }
    Assert.assertNotNull(ex);
    Assert.assertEquals(ioExceptions.size(), counter.get());
    Assert.assertEquals(2, ex.getSuppressed().length);
  }

  private void run(CloserRule closer, final Runnable runnable) throws Throwable
  {
    closer.apply(
        new Statement()
        {
          @Override
          public void evaluate()
          {
            runnable.run();
          }
        }, Description.createTestDescription(
            CloserRuleTest.class.getName(), "baseRunner", UUID.randomUUID()
        )
    ).evaluate();
  }
}
