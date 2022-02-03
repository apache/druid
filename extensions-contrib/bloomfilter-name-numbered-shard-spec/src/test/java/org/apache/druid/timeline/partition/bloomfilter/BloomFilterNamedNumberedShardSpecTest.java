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

package org.apache.druid.timeline.partition.bloomfilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.Assert;
import org.junit.Test;

public class BloomFilterNamedNumberedShardSpecTest
{
  @Test
  public void testPossibleInDomain() throws Exception
  {
    BloomFilterNamedNumberedShardSpec shardSpec = new BloomFilterNamedNumberedShardSpec(1, 2, "name", "dimension",
                                                                                        ImmutableList.of(4L, 5L, 6L)
    );
    RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("1", "2")); // {[1, 10]}
    Assert.assertTrue(shardSpec.possibleInDomain(ImmutableMap.of("xyz", rangeSet)));
    Assert.assertTrue(shardSpec.possibleInDomain(ImmutableMap.of("dimension", rangeSet)));

    rangeSet.add(Range.closed("5", "5"));
    Assert.assertTrue(shardSpec.possibleInDomain(ImmutableMap.of("dimension", rangeSet)));

    rangeSet.clear();
    rangeSet.add(Range.open("4", "5"));
    Assert.assertTrue(shardSpec.possibleInDomain(ImmutableMap.of("dimension", rangeSet)));

    rangeSet.clear();
    rangeSet.add(Range.closed("4", "4"));
    Assert.assertTrue(shardSpec.possibleInDomain(ImmutableMap.of("dimension", rangeSet)));

    rangeSet.clear();
    rangeSet.add(Range.closed("10", "10"));
    Assert.assertFalse(shardSpec.possibleInDomain(ImmutableMap.of("dimension", rangeSet)));
  }
}
