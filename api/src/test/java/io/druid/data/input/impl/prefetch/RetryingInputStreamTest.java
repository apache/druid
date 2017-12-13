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

package io.druid.data.input.impl.prefetch;

import io.druid.java.util.common.StringUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

public class RetryingInputStreamTest
{
  private static final int MAX_RETRY = 5;
  private static final int MAX_ERROR = 4;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File testFile;
  private RetryingInputStream inputStream;

  @Before
  public void setup() throws IOException
  {
    testFile = temporaryFolder.newFile();

    try (FileOutputStream fis = new FileOutputStream(testFile);
         DataOutputStream dis = new DataOutputStream(fis)) {
      for (int i = 0; i < 100; i++) {
        dis.writeInt(i);
      }
    }

    inputStream = new RetryingInputStream<>(
        testFile,
        file -> new TestInputStream(new FileInputStream(file)),
        e -> e instanceof IOException,
        MAX_RETRY
    );
  }

  @After
  public void teardown() throws IOException
  {
    inputStream.close();
    FileUtils.forceDelete(testFile);
  }

  @Test
  public void testReadRetry() throws IOException
  {
    final byte[] buf = new byte[16];
    final ByteBuffer bb = ByteBuffer.wrap(buf);

    int expected = 0;
    int read;
    while ((read = inputStream.read(buf)) != -1) {
      Assert.assertTrue(StringUtils.format("read bytes[%s] should be a multiplier of 4", read), read % 4 == 0);

      for (int i = 0; i < read; i += 4) {
        Assert.assertEquals(expected++, bb.getInt(i));
      }
    }
    Assert.assertEquals(100, expected);
  }

  private boolean throwError = true;
  private int errorCount = 0;

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
      if (throwError) {
        throwError = false;
        errorCount++;
        if (errorCount % 2 == 0) {
          throw new IOException("test retry");
        } else {
          delegate.close();
          throw new IllegalArgumentException(new SocketTimeoutException("test timeout"));
        }
      } else {
        throwError = errorCount < MAX_ERROR;
        return delegate.read();
      }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException
    {
      if (throwError) {
        throwError = false;
        errorCount++;
        if (errorCount % 2 == 0) {
          throw new IOException("test retry");
        } else {
          delegate.close();
          throw new IllegalArgumentException(new SocketTimeoutException("test timeout"));
        }
      } else {
        throwError = errorCount < MAX_ERROR;
        return delegate.read(b, off, len);
      }
    }
  }
}
