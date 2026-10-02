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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.druid.data.input.StringTuple;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringSketchMergerTest
{
  private StringSketchMerger target;

  @BeforeEach
  public void setup()
  {
    target = new StringSketchMerger();
  }

  @Test
  public void requiresStringSketch()
  {
    StringDistribution distribution = EasyMock.mock(StringDistribution.class);

    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> target.merge(distribution)
    );
    Assertions.assertTrue(exception.getMessage().contains("Only merging StringSketch instances is currently supported"));
  }

  @Test
  public void mergesCorrectly()
  {
    StringTuple string1 = StringTuple.create("a");
    StringSketch sketch1 = new StringSketch();
    sketch1.put(string1);

    StringTuple string2 = StringTuple.create("mn");
    StringSketch sketch2 = new StringSketch();
    sketch2.put(string2);

    StringTuple string3 = StringTuple.create("z");
    StringSketch sketch3 = new StringSketch();
    sketch3.put(string3);

    target.merge(sketch2);
    target.merge(sketch1);
    target.merge(sketch3);
    StringDistribution merged = target.getResult();

    PartitionBoundaries partitions = merged.getEvenPartitionsByMaxSize(1);
    Assertions.assertEquals(3, partitions.size());
    Assertions.assertNull(partitions.get(0));
    Assertions.assertEquals(string2, partitions.get(1));
    Assertions.assertNull(partitions.get(2));
  }
}
