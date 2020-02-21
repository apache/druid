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
  private static final int MAX_ERROR = 4;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File testFile;
  private DataInputStream inputStream;

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

    throwError = false;

    final InputStream retryingInputStream = new RetryingInputStream<>(
        testFile,
        new ObjectOpenFunction<File>()
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
        },
        e -> e instanceof IOException,
        MAX_RETRY
    );

    inputStream = new DataInputStream(new GZIPInputStream(retryingInputStream));

    throwError = true;
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
    for (int i = 0; i < 10000; i++) {
      Assert.assertEquals(i, inputStream.readInt());
    }
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
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
      if (throwError) {
        throwError = false;
        errorCount++;
        if (errorCount % 2 == 0) {
          throw new IOException("test retry");
        } else {
          delegate.close();
          throw new SocketException("Test Connection reset");
        }
      } else {
        throwError = errorCount < MAX_ERROR;
        return delegate.read(b, off, len);
      }
    }
  }
}
