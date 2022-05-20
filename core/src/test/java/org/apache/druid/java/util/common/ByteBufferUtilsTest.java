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

public class ByteBufferUtilsTest
{
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
  public void testUnsignedComparator()
  {
    final Comparator<ByteBuffer> comparator = ByteBufferUtils.unsignedComparator();

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

    // Tests involving the full range of bytes
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      for (byte j = Byte.MIN_VALUE; j < Byte.MAX_VALUE; j++) {
        final int cmp = Integer.compare(Byte.toUnsignedInt(i), Byte.toUnsignedInt(j));

        MatcherAssert.assertThat(
            StringUtils.format("comparison of %s to %s", Byte.toUnsignedInt(i), Byte.toUnsignedInt(j)),
            comparator.compare(
                ByteBuffer.wrap(new byte[]{i}),
                ByteBuffer.wrap(new byte[]{j})
            ),
            cmp < 0 ? Matchers.lessThan(0) : cmp > 0 ? Matchers.greaterThan(0) : Matchers.equalTo(0)
        );
      }
    }
  }
}
