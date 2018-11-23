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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class AggregatorFactoryTest
{

  @Test
  public void testMergeAggregators()
  {
    Assert.assertNull(AggregatorFactory.mergeAggregators(null));
    Assert.assertNull(AggregatorFactory.mergeAggregators(ImmutableList.of()));

    List<AggregatorFactory[]> aggregatorsToBeMerged = new ArrayList<>();

    aggregatorsToBeMerged.add(null);
    Assert.assertNull(AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    AggregatorFactory[] emptyAggFactory = new AggregatorFactory[0];

    aggregatorsToBeMerged.clear();
    aggregatorsToBeMerged.add(emptyAggFactory);
    Assert.assertArrayEquals(emptyAggFactory, AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    aggregatorsToBeMerged.clear();
    aggregatorsToBeMerged.add(emptyAggFactory);
    aggregatorsToBeMerged.add(null);
    Assert.assertNull(AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    aggregatorsToBeMerged.clear();
    AggregatorFactory[] af1 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName1")
    };
    AggregatorFactory[] af2 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName2")
    };
    Assert.assertArrayEquals(
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("name", "name")
        },
        AggregatorFactory.mergeAggregators(ImmutableList.of(af1, af2))
    );

    aggregatorsToBeMerged.clear();
    af1 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName1")
    };
    af2 = new AggregatorFactory[]{
        new DoubleMaxAggregatorFactory("name", "fieldName2")
    };
    Assert.assertNull(AggregatorFactory.mergeAggregators(ImmutableList.of(af1, af2))
    );
  }
}
