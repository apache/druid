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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryingGZIPInputStreamTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File testFile;

  private static final String ERROR_MESSAGE = "Unexpected end of ZLIB input stream";

  @Before
  public void setup() throws IOException
  {
    testFile = temporaryFolder.newFile();

    try (FileOutputStream fos = new FileOutputStream(testFile);
         GZIPOutputStream gos = new GZIPOutputStream(fos);
         DataOutputStream dos = new DataOutputStream(gos)) {
      for (int i = 0; i < 10000; i++) {
        dos.writeInt(i);
      }
    }
  }

  @After
  public void teardown() throws IOException
  {
    FileUtils.forceDelete(testFile);
  }

  @Test
  public void testReadSingleByte() throws IOException
  {
    try (
        InputStream inputStream = Files.newInputStream(testFile.toPath());
        RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(RetryingGZIPInputStream.createGZIPInputStream(inputStream), null)) {

      Assert.assertTrue(retryingInputStream.read() != -1);
    }
  }

  @Test
  public void testReadWithBuffer() throws IOException
  {
    try (InputStream inputStream = Files.newInputStream(testFile.toPath());
         RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(RetryingGZIPInputStream.createGZIPInputStream(inputStream), null)) {

      byte[] buffer = new byte[100];
      int bytesRead = retryingInputStream.read(buffer);

      assertEquals(100, bytesRead);
    }
  }

  @Test
  public void testReadWithOffsetAndLength() throws IOException
  {
    try (InputStream inputStream = Files.newInputStream(testFile.toPath());
         RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(RetryingGZIPInputStream.createGZIPInputStream(inputStream), null)) {

      byte[] buffer = new byte[200];
      int offset = 50;
      int length = 100;
      int bytesRead = retryingInputStream.read(buffer, offset, length);

      assertEquals(length, bytesRead);
    }
  }

  @Test
  public void testWaitOrThrowNonRetryableError() throws IOException
  {
    String nonRetryableErrorMessage = "Simulated IOException";
    InputStream mockInputStream = mock(InputStream.class);
    when(mockInputStream.read()).thenThrow(new IOException(nonRetryableErrorMessage));

    try (RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(mockInputStream, null)) {
      IOException ioException = assertThrows(IOException.class, retryingInputStream::read);
      Assert.assertEquals(nonRetryableErrorMessage, ioException.getMessage());
    }
  }

  @Test
  public void testWaitOrThrowRetryableError() throws IOException
  {
    InputStream mockDelegate = mock(InputStream.class);
    InputStream mockInputStream = mock(InputStream.class);
    when(mockDelegate.read()).thenThrow(new IOException(ERROR_MESSAGE)).thenReturn(1);

    try (MockedStatic<RetryingGZIPInputStream> mockedStatic = Mockito.mockStatic(RetryingGZIPInputStream.class)) {
      mockedStatic.when(() -> RetryingGZIPInputStream.createGZIPInputStream(mockInputStream)).thenReturn(mock(GZIPInputStream.class));

      try (RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(mockDelegate, mockInputStream, false)) {
        retryingInputStream.read();
      }

      verify(mockInputStream, times(1)).reset();
    }
  }

  @Test
  public void testWaitOrThrowRetryableErrorExceedingMaxRetries222() throws IOException
  {
    InputStream mockInputStream = mock(InputStream.class);
    InputStream mockDelegate = mock(InputStream.class);
    when(mockDelegate.read()).thenThrow(new IOException(ERROR_MESSAGE));

    try (MockedStatic<RetryingGZIPInputStream> mockedStatic = Mockito.mockStatic(RetryingGZIPInputStream.class)) {
      mockedStatic.when(() -> RetryingGZIPInputStream.createGZIPInputStream(mockInputStream)).thenThrow(new IOException(ERROR_MESSAGE));

      try (RetryingGZIPInputStream retryingInputStream = new RetryingGZIPInputStream(mockDelegate, mockInputStream, false)) {
        IOException ioException = assertThrows(IOException.class, retryingInputStream::read);
        Assert.assertEquals(ERROR_MESSAGE, ioException.getMessage());
      }
      verify(mockInputStream, times(10)).reset();
    }
  }
}
