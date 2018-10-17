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

package org.apache.druid.java.util.http.client.io;

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class AppendableByteArrayInputStreamTest
{

  @Test
  public void testSingleByteArray() throws Exception
  {
    byte[][] bytesToWrite = new byte[][]{{0, 1, 2, 3, 4, 5, 6}};

    testAll(bytesToWrite, bytesToWrite[0]);
  }

  @Test
  public void testMultiByteArray() throws Exception
  {
    byte[] expectedBytes = new byte[]{0, 1, 2, 3, 4, 5, 6};

    testAll(new byte[][]{{0, 1, 2, 3}, {4, 5, 6}}, expectedBytes);
    testAll(new byte[][]{{0, 1}, {2, 3}, {4, 5, 6}}, expectedBytes);
    testAll(new byte[][]{{0}, {1}, {2}, {3}, {4}, {5}, {6}}, expectedBytes);
  }

  public void testAll(byte[][] writtenBytes, byte[] expectedBytes) throws Exception
  {
    testFullRead(writtenBytes, expectedBytes);
    testIndividualRead(writtenBytes, expectedBytes);
  }

  public void testIndividualRead(byte[][] writtenBytes, byte[] expectedBytes) throws IOException
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    for (byte[] writtenByte : writtenBytes) {
      in.add(writtenByte);
    }

    for (int i = 0; i < expectedBytes.length; i++) {
      final int readByte = in.read();
      if (expectedBytes[i] != (byte) readByte) {
        Assert.assertEquals(StringUtils.format("%s[%d]", Arrays.toString(expectedBytes), i), expectedBytes[i], readByte);
      }
    }
  }

  public void testFullRead(byte[][] writtenBytes, byte[] expectedBytes) throws IOException
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    byte[] readBytes = new byte[expectedBytes.length];

    for (byte[] writtenByte : writtenBytes) {
      in.add(writtenByte);
    }
    Assert.assertEquals(readBytes.length, in.read(readBytes));
    Assert.assertArrayEquals(expectedBytes, readBytes);
  }

  @Test
  public void testReadsAndWritesInterspersed() throws Exception
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    in.add(new byte[]{0, 1, 2});

    byte[] readBytes = new byte[3];
    Assert.assertEquals(3, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{0, 1, 2}, readBytes);

    in.add(new byte[]{3, 4});
    in.add(new byte[]{5, 6, 7});

    readBytes = new byte[5];
    Assert.assertEquals(5, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{3, 4, 5, 6, 7}, readBytes);
  }

  @Test
  public void testReadLessThanWritten() throws Exception
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    in.add(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    byte[] readBytes = new byte[4];

    Assert.assertEquals(4, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{0, 1, 2, 3}, readBytes);

    Assert.assertEquals(4, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{4, 5, 6, 7}, readBytes);

    Assert.assertEquals(2, in.read(readBytes, 0, 2));
    Assert.assertArrayEquals(new byte[]{8, 9, 6, 7}, readBytes);
  }

  @Test
  public void testReadLessThanWrittenMultiple() throws Exception
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    in.add(new byte[]{0, 1, 2});
    in.add(new byte[]{3, 4, 5});
    in.add(new byte[]{6, 7});
    in.add(new byte[]{8, 9});

    byte[] readBytes = new byte[4];

    Assert.assertEquals(4, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{0, 1, 2, 3}, readBytes);

    Assert.assertEquals(4, in.read(readBytes));
    Assert.assertArrayEquals(new byte[]{4, 5, 6, 7}, readBytes);

    Assert.assertEquals(2, in.read(readBytes, 0, 2));
    Assert.assertArrayEquals(new byte[]{8, 9, 6, 7}, readBytes);
  }

  @Test
  public void testBlockingRead() throws Exception
  {
    final AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    in.add(new byte[]{0, 1, 2, 3, 4});

    Assert.assertEquals(5, in.available());

    Future<byte[]> bytesFuture = Executors.newSingleThreadExecutor().submit(
        new Callable<byte[]>()
        {
          @Override
          public byte[] call() throws Exception
          {
            byte[] readBytes = new byte[10];
            in.read(readBytes);
            return readBytes;
          }
        }
    );

    int count = 0;
    while (in.available() != 0) {
      if (count >= 100) {
        Assert.fail("available didn't become 0 fast enough.");
      }
      count++;
      Thread.sleep(10);
    }

    in.add(new byte[]{5, 6, 7, 8, 9, 10});

    count = 0;
    while (in.available() != 1) {
      if (count >= 100) {
        Assert.fail("available didn't become 1 fast enough.");
      }
      count++;
      Thread.sleep(10);
    }

    Assert.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, bytesFuture.get());
    Assert.assertEquals(10, in.read());
    Assert.assertEquals(0, in.available());
  }

  @Test
  public void testAddEmptyByteArray() throws Exception
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();

    in.add(new byte[]{});
    in.add(new byte[]{1});
    in.add(new byte[]{});
    in.done();

    Assert.assertEquals(1, in.available());
    Assert.assertEquals(1, in.read());
    Assert.assertEquals(0, in.available());
    Assert.assertEquals(-1, in.read());
  }

  @Test
  public void testExceptionUnblocks() throws InterruptedException
  {
    final AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    in.add(new byte[]{});
    in.add(new byte[]{1});
    in.add(new byte[]{});
    final AtomicReference<IOException> exceptionThrown = new AtomicReference<IOException>();
    final CountDownLatch latch = new CountDownLatch(1);
    Executors.newSingleThreadExecutor().submit(
        new Callable()
        {
          @Override
          public byte[] call()
          {
            try {
              byte[] readBytes = new byte[10];
              while (in.read(readBytes) != -1) {
              }
              return readBytes;
            }
            catch (IOException e) {
              exceptionThrown.set(e);
              latch.countDown();
            }
            return null;
          }
        }
    );

    Exception expected = new Exception();
    in.exceptionCaught(expected);

    latch.await();
    Assert.assertEquals(expected, exceptionThrown.get().getCause());

    try {
      in.read();
      Assert.fail();
    }
    catch (IOException thrown) {
      Assert.assertEquals(expected, thrown.getCause());
    }

  }
}
