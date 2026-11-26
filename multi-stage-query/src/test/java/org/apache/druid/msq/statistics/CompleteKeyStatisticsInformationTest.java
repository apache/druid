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

package org.apache.druid.msq.statistics;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

public class CompleteKeyStatisticsInformationTest
{
  @Test
  public void test_spaceReductionFactorForFetch_singleTimeChunk_isHighestFactorOfAnyWorker()
  {
    final CompleteKeyStatisticsInformation information = new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    information.mergePartialInformation(0, new PartialKeyStatisticsInformation(ImmutableSet.of(0L), false, 1, 2));
    information.mergePartialInformation(1, new PartialKeyStatisticsInformation(ImmutableSet.of(0L), false, 1, 7));
    information.mergePartialInformation(2, new PartialKeyStatisticsInformation(ImmutableSet.of(0L), false, 1, 0));
    information.complete();

    Assertions.assertEquals(7, information.getSpaceReductionFactorForFetch());
  }

  @Test
  public void test_spaceReductionFactorForFetch_notComplete_isZero()
  {
    final CompleteKeyStatisticsInformation information = new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    information.mergePartialInformation(0, new PartialKeyStatisticsInformation(ImmutableSet.of(0L), false, 1, 7));

    Assertions.assertEquals(0, information.getSpaceReductionFactorForFetch());
  }

  @Test
  public void test_spaceReductionFactorForFetch_severalTimeChunks_isZero()
  {
    // A worker reports one factor for all its time chunks, so it can't say how far each chunk may be downsampled.
    final CompleteKeyStatisticsInformation information = new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    information.mergePartialInformation(0, new PartialKeyStatisticsInformation(ImmutableSet.of(0L, 1L), false, 1, 7));
    information.mergePartialInformation(1, new PartialKeyStatisticsInformation(ImmutableSet.of(0L), false, 1, 3));
    information.complete();

    Assertions.assertEquals(0, information.getSpaceReductionFactorForFetch());
  }

  @Test
  public void test_spaceReductionFactorForFetch_noData_isZero()
  {
    final CompleteKeyStatisticsInformation information = new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    information.complete();

    Assertions.assertEquals(0, information.getSpaceReductionFactorForFetch());
  }
}
