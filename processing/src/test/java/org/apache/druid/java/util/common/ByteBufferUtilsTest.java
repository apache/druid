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

package org.apache.druid.java.util.common;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.druid.collections.ResourceHolder;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ByteBufferUtilsTest
{
  private static final List<String> COMPARE_TEST_STRINGS = ImmutableList.of(
      "（請參見已被刪除版本）",
      "請參見已被刪除版本",
      "שָׁלוֹם",
      "＋{{[[Template:別名重定向|別名重定向]]}}",
      "\uD83D\uDC4D\uD83D\uDC4D\uD83D\uDC4D",
      "\uD83D\uDCA9",
      "",
      "f",
      "fo",
      "\uD83D\uDE42",
      "\uD83E\uDEE5",
      "\uD83E\uDD20",
      "quick",
      "brown",
      "fox"
  );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAllocateDirect()
  {
    final int sz = 10;

    try (final ResourceHolder<ByteBuffer> holder = ByteBufferUtils.allocateDirect(sz)) {
      final ByteBuffer buf = holder.get();
      Assert.assertTrue(buf.isDirect());
      Assert.assertEquals(sz, buf.remaining());
    }
  }

  @Test
  public void testUnmapDoesntCrashJVM() throws Exception
  {
    final File file = temporaryFolder.newFile("some_mmap_file");
    try (final OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
      final byte[] data = new byte[4096];
      Arrays.fill(data, (byte) 0x5A);
      os.write(data);
    }
    final MappedByteBuffer mappedByteBuffer = Files.map(file);
    Assert.assertEquals((byte) 0x5A, mappedByteBuffer.get(0));
    ByteBufferUtils.unmap(mappedByteBuffer);
    ByteBufferUtils.unmap(mappedByteBuffer);
  }

  @Test
  public void testFreeDoesntCrashJVM()
  {
    final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);
    ByteBufferUtils.free(directBuffer);
    ByteBufferUtils.free(directBuffer);

    final ByteBuffer heapBuffer = ByteBuffer.allocate(4096);
    ByteBufferUtils.free(heapBuffer);
  }

  @Test
  @SuppressWarnings("EqualsWithItself")
  public void testUtf8Comparator()
  {
    final Comparator<ByteBuffer> comparator = ByteBufferUtils.utf8Comparator();

    // Tests involving null
    MatcherAssert.assertThat(comparator.compare(null, null), Matchers.equalTo(0));
    MatcherAssert.assertThat(comparator.compare(null, ByteBuffer.allocate(0)), Matchers.lessThan(0));
    MatcherAssert.assertThat(comparator.compare(ByteBuffer.allocate(0), null), Matchers.greaterThan(0));
    MatcherAssert.assertThat(comparator.compare(null, ByteBuffer.allocate(1)), Matchers.lessThan(0));
    MatcherAssert.assertThat(comparator.compare(ByteBuffer.allocate(1), null), Matchers.greaterThan(0));
    MatcherAssert.assertThat(comparator.compare(null, ByteBuffer.wrap(new byte[]{-1})), Matchers.lessThan(0));
    MatcherAssert.assertThat(comparator.compare(ByteBuffer.wrap(new byte[]{-1}), null), Matchers.greaterThan(0));

    // Tests involving buffers of different lengths
    MatcherAssert.assertThat(
        comparator.compare(
            ByteBuffer.wrap(new byte[]{1, 2, 3}),
            ByteBuffer.wrap(new byte[]{1, 2, 3, 4})
        ),
        Matchers.lessThan(0)
    );

    MatcherAssert.assertThat(
        comparator.compare(
            ByteBuffer.wrap(new byte[]{1, 2, 3, 4}),
            ByteBuffer.wrap(new byte[]{1, 2, 3})
        ),
        Matchers.greaterThan(0)
    );

    for (final String string1 : COMPARE_TEST_STRINGS) {
      for (final String string2 : COMPARE_TEST_STRINGS) {
        final byte[] utf8Bytes1 = StringUtils.toUtf8(string1);
        final byte[] utf8Bytes2 = StringUtils.toUtf8(string2);
        final ByteBuffer utf8ByteBuffer1 = ByteBuffer.allocate(utf8Bytes1.length + 2);
        final ByteBuffer utf8ByteBuffer2 = ByteBuffer.allocate(utf8Bytes2.length + 2);
        utf8ByteBuffer1.position(1);
        utf8ByteBuffer1.put(utf8Bytes1, 0, utf8Bytes1.length).position(utf8Bytes1.length);
        utf8ByteBuffer1.position(1).limit(1 + utf8Bytes1.length);
        utf8ByteBuffer2.position(1);
        utf8ByteBuffer2.put(utf8Bytes2, 0, utf8Bytes2.length).position(utf8Bytes2.length);
        utf8ByteBuffer2.position(1).limit(1 + utf8Bytes2.length);

        final int compareByteBufferUtilsUtf8 = ByteBufferUtils.utf8Comparator().compare(
            utf8ByteBuffer1,
            utf8ByteBuffer2
        );

        Assert.assertEquals(
            StringUtils.format(
                "compareByteBufferUtilsUtf8(byte[]) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            ),
            (int) Math.signum(string1.compareTo(string2)),
            (int) Math.signum(compareByteBufferUtilsUtf8)
        );
      }
    }
  }
}
