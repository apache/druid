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

package io.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;

/**
 */
public class VSizeIndexedTest
{
  @Test
  public void testSanity() throws Exception
  {
    List<int[]> someInts = Arrays.asList(
        new int[]{1, 2, 3, 4, 5},
        new int[]{6, 7, 8, 9, 10},
        new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    );

    VSizeIndexed indexed = VSizeIndexed.fromIterable(
        Iterables.transform(
            someInts,
            new Function<int[], VSizeIndexedInts>()
            {
              @Override
              public VSizeIndexedInts apply(int[] input)
              {
                return VSizeIndexedInts.fromArray(input, 20);
              }
            }
        )
    );

    assertSame(someInts, indexed);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    indexed.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(indexed.getSerializedSize(), bytes.length);
    VSizeIndexed deserializedIndexed = VSizeIndexed.readFromByteBuffer(ByteBuffer.wrap(bytes));

    assertSame(someInts, deserializedIndexed);
  }

  private void assertSame(List<int[]> someInts, VSizeIndexed indexed)
  {
    Assert.assertEquals(3, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      final int[] ints = someInts.get(i);
      final VSizeIndexedInts vSizeIndexedInts = indexed.get(i);

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      Assert.assertEquals(1, vSizeIndexedInts.getNumBytes());
      for (int j = 0; j < ints.length; j++) {
          Assert.assertEquals(ints[j], vSizeIndexedInts.get(j));
      }
    }
  }
}
