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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RetryingInputStreamTest
{
  private static final int MAX_RETRY = 5;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File testFile;

  private int readBytesBeforeExceptions = 0;
  private int throwCustomExceptions = 0;
  private int throwIOExceptions = 0;


  private final ObjectOpenFunction<File> objectOpenFunction = new ObjectOpenFunction<File>()
  {
    @Override
    public InputStream open(File object) throws IOException
    {
      return new TestInputStream(new FileInputStream(object));
    }

    @Override
    public InputStream open(File object, long start) throws IOException
    {
      final FileInputStream fis = new FileInputStream(object);
      Preconditions.checkState(fis.skip(start) == start);
      return new TestInputStream(fis);
    }
  };

  @Before
  public void setup() throws IOException
  {
    testFile = temporaryFolder.newFile();

    try (FileOutputStream fis = new FileOutputStream(testFile);
         GZIPOutputStream gis = new GZIPOutputStream(fis);
         DataOutputStream dis = new DataOutputStream(gis)) {
      for (int i = 0; i < 10000; i++) {
        dis.writeInt(i);
      }
    }

    readBytesBeforeExceptions = 0;
    throwCustomExceptions = 0;
    throwIOExceptions = 0;
  }

  @After
  public void teardown() throws IOException
  {
    FileUtils.forceDelete(testFile);
  }

  @Test
  public void testThrowsOnIOException() throws IOException
  {
    throwIOExceptions = 1;
    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> false, // will not retry
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    Assert.assertThrows(
        IOException.class,
        () -> retryHelper(retryingInputStream)
    );

    Assert.assertEquals(0, throwIOExceptions);
  }

  @Test
  public void testRetryOnCustomException() throws IOException
  {
    throwCustomExceptions = 1;
    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> t instanceof CustomException,
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    retryHelper(retryingInputStream);

    Assert.assertEquals(0, throwCustomExceptions);
  }

  @Test
  public void testThrowsOnCustomException() throws IOException
  {
    throwCustomExceptions = 1;
    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> false, // will not retry
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    final IOException e = Assert.assertThrows(
        IOException.class,
        () -> retryHelper(retryingInputStream)
    );

    Assert.assertEquals(0, throwCustomExceptions);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(CustomException.class));
  }

  @Test
  public void testResumeAfterExceptions() throws IOException
  {
    readBytesBeforeExceptions = 1000;
    throwCustomExceptions = 100;

    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> true, // always retry
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    retryHelper(retryingInputStream);

    // Tried more than MAX_RETRY times because progress was being made. (MAX_RETRIES applies to each call individually.)
    Assert.assertEquals(81, throwCustomExceptions);
  }

  @Test
  public void testTooManyExceptions() throws IOException
  {
    throwIOExceptions = 11;
    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> t instanceof IOException,
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    Assert.assertThrows(
        IOException.class,
        () -> retryHelper(retryingInputStream)
    );

    Assert.assertEquals(6, throwIOExceptions);
  }

  @Test
  public void testIOExceptionNotRetriableRead() throws IOException
  {
    throwCustomExceptions = 1;
    throwIOExceptions = 1;
    final RetryingInputStream<File> retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> t instanceof IOException || t instanceof CustomException,
        MAX_RETRY
    );

    retryingInputStream.setNoWait();
    retryHelper(retryingInputStream);

    Assert.assertEquals(0, throwCustomExceptions);
    Assert.assertEquals(0, throwIOExceptions);
  }

  private void retryHelper(RetryingInputStream<File> retryingInputStream) throws IOException
  {
    try (DataInputStream inputStream = new DataInputStream(new GZIPInputStream(retryingInputStream))) {
      for (int i = 0; i < 10000; i++) {
        Assert.assertEquals(i, inputStream.readInt());
      }

      Assert.assertEquals(-1, inputStream.read());
    }
  }

  private class TestInputStream extends FilterInputStream
  {
    private long bytesRead = 0;

    TestInputStream(InputStream delegate)
    {
      super(delegate);
    }

    @Override
    public int read() throws IOException
    {
      possiblyThrowException();
      final int r = super.read();
      bytesRead++;
      return r;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
      return read(b, 0, b.length);
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException
    {
      possiblyThrowException();

      final int lenToUse;

      if (throwIOExceptions > 0 || throwCustomExceptions > 0) {
        lenToUse = Ints.checkedCast(Math.min(len, readBytesBeforeExceptions - bytesRead));
      } else {
        lenToUse = len;
      }

      final int r = super.read(b, off, lenToUse);
      bytesRead += r;
      return r;
    }

    private void possiblyThrowException() throws IOException
    {
      if (bytesRead >= readBytesBeforeExceptions) {
        if (throwIOExceptions > 0) {
          throwIOExceptions--;
          throw new IOException("test retry");
        } else if (throwCustomExceptions > 0) {
          throwCustomExceptions--;
          RuntimeException e = new RuntimeException();
          throw new CustomException("I am a custom retryable exception", e);
        }
      }
    }
  }

  private static class CustomException extends RuntimeException
  {
    public CustomException(String err, Throwable t)
    {
      super(err, t);
    }
  }
}
