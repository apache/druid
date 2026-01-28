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

package org.apache.druid.msq.shuffle.output;

import com.google.common.collect.ImmutableList;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ByteChunksInputStreamTest
{
  private final List<byte[]> chunks = ImmutableList.of(
      new byte[]{-128, -127, -1, 0, 1, 126, 127},
      new byte[]{0},
      new byte[]{3, 4, 5}
  );

  @Test
  public void test_read_fromStart() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 0)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      int c;
      while ((c = in.read()) != -1) {
        MatcherAssert.assertThat("InputStream#read contract", c, Matchers.greaterThanOrEqualTo(0));
        baos.write(c);
      }

      Assert.assertArrayEquals(chunksSubset(0), baos.toByteArray());
    }
  }

  @Test
  public void test_read_fromSecondByte() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 1)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      int c;
      while ((c = in.read()) != -1) {
        MatcherAssert.assertThat("InputStream#read contract", c, Matchers.greaterThanOrEqualTo(0));
        baos.write(c);
      }

      Assert.assertArrayEquals(chunksSubset(1), baos.toByteArray());
    }
  }

  @Test
  public void test_read_array1_fromStart() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 0)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[2];

      int r;
      while ((r = in.read(buf, 1, 1)) != -1) {
        Assert.assertEquals("InputStream#read bytes read", 1, r);
        baos.write(buf, 1, 1);
      }

      Assert.assertArrayEquals(chunksSubset(0), baos.toByteArray());
    }
  }

  @Test
  public void test_read_array1_fromSecondByte() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 1)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[2];

      int r;
      while ((r = in.read(buf, 1, 1)) != -1) {
        Assert.assertEquals("InputStream#read bytes read", 1, r);
        baos.write(buf, 1, 1);
      }

      Assert.assertArrayEquals(chunksSubset(1), baos.toByteArray());
    }
  }

  @Test
  public void test_read_array3_fromStart() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 0)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[5];

      int r;
      while ((r = in.read(buf, 2, 3)) != -1) {
        baos.write(buf, 2, r);
      }

      Assert.assertArrayEquals(chunksSubset(0), baos.toByteArray());
    }
  }

  @Test
  public void test_read_array3_fromSecondByte() throws IOException
  {
    try (final InputStream in = new ByteChunksInputStream(chunks, 1)) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[6];

      int r;
      while ((r = in.read(buf, 2, 3)) != -1) {
        baos.write(buf, 2, r);
      }

      Assert.assertArrayEquals(chunksSubset(1), baos.toByteArray());
    }
  }

  private byte[] chunksSubset(final int positionInFirstChunk)
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    for (int chunk = 0, p = positionInFirstChunk; chunk < chunks.size(); chunk++, p = 0) {
      baos.write(chunks.get(chunk), p, chunks.get(chunk).length - p);
    }

    return baos.toByteArray();
  }
}
