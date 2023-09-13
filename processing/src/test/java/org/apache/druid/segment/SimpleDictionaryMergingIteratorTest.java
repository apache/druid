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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SimpleDictionaryMergingIteratorTest
{
  @Test
  public void testMergingIterator()
  {
    final Indexed[] sortedLookups = new Indexed[]{
        new ListIndexed(null, "", "null", "z"),
        new ListIndexed("", "a", "b", "c", "d", "e", "f", "g", "h"),
        new ListIndexed(null, "b", "c", "null", "z"),
        new ListIndexed(null, "hello")
    };
    SimpleDictionaryMergingIterator<String> dictionaryMergeIterator = new SimpleDictionaryMergingIterator<>(
        sortedLookups,
        AutoTypeColumnMerger.STRING_MERGING_COMPARATOR
    );

    List<String> expectedSequence = Lists.newArrayListWithExpectedSize(13);
    expectedSequence.add(null);
    expectedSequence.addAll(ImmutableList.of("", "a", "b", "c", "d", "e", "f", "g", "h", "hello", "null", "z"));

    List<String> actualSequence = Lists.newArrayListWithExpectedSize(13);
    while (dictionaryMergeIterator.hasNext()) {
      actualSequence.add(dictionaryMergeIterator.next());
    }
    Assert.assertEquals(expectedSequence, actualSequence);
  }
}
