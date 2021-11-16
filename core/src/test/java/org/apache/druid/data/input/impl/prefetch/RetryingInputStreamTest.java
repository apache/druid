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

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.RetryingInputStream;
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
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RetryingInputStreamTest
{
  private static final int MAX_RETRY = 5;
  private static final int MAX_ERROR = 6;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File testFile;

  private boolean throwError = false;
  private boolean throwSocketException = false;
  private boolean throwCustomException = false;
  private boolean throwIOException = false;
  private int errorCount = 0;


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
    throwError = true;
    throwSocketException = false;
    throwCustomException = false;
    throwIOException = false;
    errorCount = 0;
  }

  @After
  public void teardown() throws IOException
  {
    FileUtils.forceDelete(testFile);
  }


  @Test
  public void testDefaultsRead() throws IOException
  {
    throwSocketException = true;
    throwIOException = true;
    final InputStream retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        null, // will retry if exception thrown is IOException
        null, // will enable reset using default logic
        MAX_RETRY
    );
    retryHelper(retryingInputStream);
  }

  @Test
  public void testCustomResetRead() throws IOException
  {
    throwCustomException = true;
    throwIOException = true;
    final InputStream retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        null, // will retry if exception thrown is IOException
        t -> t instanceof CustomException,
        MAX_RETRY
    );
    retryHelper(retryingInputStream);
  }

  @Test(expected = IOException.class)
  public void testCustomResetReadThrows() throws IOException
  {
    throwCustomException = true;
    throwIOException = true;
    final InputStream retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        null, // will retry if exception thrown is IOException
        null, // since there is no custom reset lambda it will fail when the custom exception is thrown
        MAX_RETRY
    );
    retryHelper(retryingInputStream);
  }

  @Test(expected = IOException.class)
  public void testIOExceptionNotRetriableReadThrows() throws IOException
  {
    throwCustomException = true;
    throwIOException = true;
    final InputStream retryingInputStream = new RetryingInputStream<>(
        testFile,
        objectOpenFunction,
        t -> t instanceof CustomException, // this is a weird predicate for retry but just using it to make a point
        t -> t instanceof CustomException,
        MAX_RETRY
    );
    retryHelper(retryingInputStream);
  }

  private void retryHelper(InputStream retryingInputStream) throws IOException
  {
    DataInputStream inputStream = new DataInputStream(new GZIPInputStream(retryingInputStream));
    try {
      for (int i = 0; i < 10000; i++) {
        Assert.assertEquals(i, inputStream.readInt());
      }
    }
    finally {
      inputStream.close();
    }
  }

  private class TestInputStream extends InputStream
  {
    private final InputStream delegate;

    TestInputStream(InputStream delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException
    {
      return delegate.read();
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException
    {
      errorCount++;
      if (throwIOException) {
        throwIOException = false;
        throw new IOException("test retry");
      } else if (throwCustomException) {
        throwCustomException = false;
        RuntimeException e = new RuntimeException();
        throw new CustomException("I am a custom ResettableException", e);
      } else if (throwSocketException) {
        throwSocketException = false;
        delegate.close();
        throw new SocketException("Test Connection reset");
      } else {
        if (errorCount >= MAX_ERROR) {
          throwError = false;
        }
        return delegate.read(b, off, len);
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
