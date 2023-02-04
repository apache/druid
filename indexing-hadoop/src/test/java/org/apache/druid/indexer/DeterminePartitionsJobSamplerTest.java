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

package org.apache.druid.indexer;

import org.junit.Assert;
import org.junit.Test;

public class DeterminePartitionsJobSamplerTest
{
  @Test
  public void testSampled()
  {
    int sample = 10;
    int targetPartitionSize = 1000000;
    int maxRowsPerSegment = 5000000;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        sample,
        targetPartitionSize,
        maxRowsPerSegment
    );
    Assert.assertEquals(100000, sampler.getSampledTargetPartitionSize());
    Assert.assertEquals(500000, sampler.getSampledMaxSegmentSize());
  }

  @Test
  public void testNotSampled()
  {
    int sample = 0;
    int targetPartitionSize = 1000000;
    int maxRowsPerSegment = 5000000;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        sample,
        targetPartitionSize,
        maxRowsPerSegment
    );
    Assert.assertEquals(targetPartitionSize, sampler.getSampledTargetPartitionSize());
    Assert.assertEquals(maxRowsPerSegment, sampler.getSampledMaxSegmentSize());
  }
}
