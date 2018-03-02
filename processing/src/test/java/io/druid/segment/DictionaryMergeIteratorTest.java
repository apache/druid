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

package io.druid.segment;

import com.google.common.collect.Iterators;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DictionaryMergeIteratorTest
{

  @Test
  public void basicTest()
  {
    // a b c d e f
    String[] s1 = {"a", "c", "d", "e"};   // 0 2 3 4
    String[] s2 = {"b", "c", "e"};        // 1 2 4
    String[] s3 = {"a", "d", "f"};        // 0 3 5
    String[] s4 = {"a", "b", "c"};
    String[] s5 = {"a", "b", "c", "d", "e", "f"};
    Indexed<String> i1 = new ArrayIndexed<String>(s1, String.class);
    Indexed<String> i2 = new ArrayIndexed<String>(s2, String.class);
    Indexed<String> i3 = new ArrayIndexed<String>(s3, String.class);
    Indexed<String> i4 = new ArrayIndexed<String>(s4, String.class);
    Indexed<String> i5 = new ArrayIndexed<String>(s5, String.class);

    IndexMerger.DictionaryMergeIterator iterator = new IndexMerger.DictionaryMergeIterator(new Indexed[]{i1, i2, i3, i4, i5}, false);

    Assert.assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f"}, Iterators.toArray(iterator, String.class));

    Assert.assertArrayEquals(new int[] {0, 2, 3, 4}, iterator.conversions[0].array());
    Assert.assertArrayEquals(new int[] {1, 2, 4}, iterator.conversions[1].array());
    Assert.assertArrayEquals(new int[] {0, 3, 5}, iterator.conversions[2].array());
    Assert.assertArrayEquals(new int[] {0, 1, 2}, iterator.conversions[3].array());
    Assert.assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5}, iterator.conversions[4].array());

    Assert.assertTrue(iterator.needConversion(0));
    Assert.assertTrue(iterator.needConversion(1));
    Assert.assertTrue(iterator.needConversion(2));
    Assert.assertFalse(iterator.needConversion(3));
    Assert.assertFalse(iterator.needConversion(4));
  }
}
