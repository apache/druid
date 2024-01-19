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

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class DeterminePartitionsJobSamplerTest
{
  @Test
  public void testSampled()
  {
    int samplingFactor = 10;
    int targetPartitionSize = 1000000;
    int maxRowsPerSegment = 5000000;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        samplingFactor,
        targetPartitionSize,
        maxRowsPerSegment
    );
    Assert.assertEquals(100000, sampler.getSampledTargetPartitionSize());
    Assert.assertEquals(500000, sampler.getSampledMaxRowsPerSegment());
  }

  @Test
  public void testNotSampled()
  {
    int samplingFactor = 0;
    int targetPartitionSize = 1000000;
    int maxRowsPerSegment = 5000000;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        samplingFactor,
        targetPartitionSize,
        maxRowsPerSegment
    );
    Assert.assertEquals(targetPartitionSize, sampler.getSampledTargetPartitionSize());
    Assert.assertEquals(maxRowsPerSegment, sampler.getSampledMaxRowsPerSegment());
  }

  @Test
  public void testShouldEmitRowByHash()
  {
    int samplingFactor = 10;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        samplingFactor,
        1000,
        5000
    );
    long total = 100000L;
    long hit = 0;
    for (long i = 0; i < total; i++) {
      String str = UUID.randomUUID().toString();
      if (sampler.shouldEmitRow(str.getBytes(StandardCharsets.UTF_8))) {
        hit++;
      }
    }
    double expect = total * 1.0 / samplingFactor;
    double error = Math.abs(hit - expect) / expect;
    Assert.assertTrue(String.valueOf(error), error < 0.02);
  }

  @Test
  public void testShouldEmitRowByRandom()
  {
    int samplingFactor = 10;
    DeterminePartitionsJobSampler sampler = new DeterminePartitionsJobSampler(
        samplingFactor,
        1000,
        5000
    );
    long total = 1000000L;
    long hit = 0;
    for (long i = 0; i < total; i++) {
      if (sampler.shouldEmitRow()) {
        hit++;
      }
    }
    double expect = total * 1.0 / samplingFactor;
    double error = Math.abs(hit - expect) / expect;
    Assert.assertTrue(String.valueOf(error), error < 0.02);
  }
}
