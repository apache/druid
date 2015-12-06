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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Runnables;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nullable;
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
  public final CloserRule closer = new CloserRule(true);
  @Test
  public void testCloses() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            closed.set(true);
          }
        }
    );
    run(closer, Runnables.doNothing());
    Assert.assertTrue(closed.get());
  }

  @Test
  public void testAutoCloses() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    closer.closeLater(
        new AutoCloseable()
        {
          @Override
          public void close() throws Exception
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
          public void close() throws IOException
          {
            closed.set(true);
          }
        }
    );

    final String msg = "You can't divide by zero, you can only take the limit of such!";
    Exception ex = null;
    try {
      run(
          closer, new Runnable()
          {
            @Override
            public void run()
            {
              throw new ArithmeticException(msg);
            }
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
  public void testAddsSuppressed() throws Throwable
  {
    final CloserRule closer = new CloserRule(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
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
            closed.set(true);
          }
        }
    );

    final String msg = "You can't divide by zero, you can only take the limit of such!";
    Throwable ex = null;
    try {
      run(
          closer, new Runnable()
          {
            @Override
            public void run()
            {
              throw new ArithmeticException(msg);
            }
          }
      );
    }
    catch (Throwable e) {
      ex = e;
    }
    Assert.assertTrue(closed.get());
    Assert.assertNotNull(ex);
    Assert.assertTrue(ex instanceof ArithmeticException);
    Assert.assertEquals(msg, ex.getMessage());
    Assert.assertEquals(
        ImmutableList.of(ioExceptionMsg),
        Lists.transform(
            Arrays.asList(ex.getSuppressed()),
            new Function<Throwable, String>()
            {
              @Nullable
              @Override
              public String apply(@Nullable Throwable input)
              {
                if (input == null) {
                  return null;
                }
                return input.getSuppressed()[0].getMessage();
              }
            }
        )
    );
  }

  @Test
  public void testThrowsCloseException()
  {
    final CloserRule closer = new CloserRule(true);
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
    Throwable ex = null;
    try {
      run(closer, Runnables.doNothing());
    }
    catch (Throwable throwable) {
      ex = throwable;
    }
    Assert.assertNotNull(ex);
    ex = ex.getSuppressed()[0];
    Assert.assertNotNull(ex);
    Assert.assertTrue(ex instanceof IOException);
    Assert.assertEquals(ioExceptionMsg, ex.getMessage());
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
        new AutoCloseable()
        {
          @Override
          public void close() throws Exception
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
    final List<IOException> ioExceptions = Arrays.<IOException>asList(
        new IOException(ioExceptionMsg),
        null,
        new IOException(ioExceptionMsg),
        null,
        new IOException(ioExceptionMsg),
        null
    );
    for(final IOException throwable : ioExceptions){
      closer.closeLater(
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              counter.incrementAndGet();
              if(throwable != null){
                throw throwable;
              }
            }
          }
      );
    }
    for(final IOException throwable : ioExceptions){
      closer.closeLater(
          new AutoCloseable()
          {
            @Override
            public void close() throws Exception
            {
              counter.incrementAndGet();
              if(throwable != null){
                throw throwable;
              }
            }
          }
      );
    }
    Throwable ex = null;
    try {
      run(closer, Runnables.doNothing());
    }catch (Throwable throwable) {
      ex = throwable;
    }
    Assert.assertNotNull(ex);
    Assert.assertEquals(ioExceptions.size() * 2, counter.get());
    Assert.assertEquals(ioExceptions.size(), ex.getSuppressed().length);
  }

  @Ignore // This one doesn't quite work right, it will throw the IOException, but JUnit doesn't detect it properly and treats it as suppressed instead
  @Test(expected = IOException.class)
  public void testCloserException()
  {
    closer.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            throw new IOException("This is a test");
          }
        }
    );
  }

  private void run(CloserRule closer, final Runnable runnable) throws Throwable
  {
    closer.apply(
        new Statement()
        {
          @Override
          public void evaluate() throws Throwable
          {
            runnable.run();
          }
        }, Description.createTestDescription(
            CloserRuleTest.class.getCanonicalName(), "baseRunner", UUID.randomUUID()
        )
    ).evaluate();
  }
}
