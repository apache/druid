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

package org.apache.druid.msq.exec;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.junit.Assert;
import org.junit.Test;

public class WorkerMemoryParametersTest
{
  @Test
  public void test_oneWorkerInJvm_alone()
  {
    Assert.assertEquals(parameters(1, 27, 149_410_000, 66_900_000), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 8, 99_160_000, 44_400_000), compute(1_000_000_000, 1, 2, 1));
    Assert.assertEquals(parameters(3, 2, 58_960_000, 26_400_000), compute(1_000_000_000, 1, 4, 1));
    Assert.assertEquals(parameters(2, 2, 32_160_000, 14_400_000), compute(1_000_000_000, 1, 8, 1));
    Assert.assertEquals(parameters(1, 3, 21_852_307, 9_784_615), compute(1_000_000_000, 1, 12, 1));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(1_000_000_000, 1, 32), e.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 74, 267_330_000, 119_700_000), compute(2_000_000_000, 1, 1, 200));
    Assert.assertEquals(parameters(2, 24, 133_330_000, 59_700_000), compute(2_000_000_000, 1, 2, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 4, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 64), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 137, 609_030_000, 272_700_000), compute(9_000_000_000L, 4, 1, 200));
    Assert.assertEquals(parameters(2, 56, 485_080_000, 217_200_000), compute(9_000_000_000L, 4, 2, 200));
    Assert.assertEquals(parameters(4, 20, 330_142_500, 147_825_000), compute(9_000_000_000L, 4, 4, 200));
    Assert.assertEquals(parameters(4, 13, 175_205_000, 78_450_000), compute(9_000_000_000L, 4, 8, 200));
    Assert.assertEquals(parameters(4, 7, 51_255_000, 22_950_000), compute(9_000_000_000L, 4, 16, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(8_000_000_000L, 4, 32, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 107), e.getFault());

    // Make sure 107 actually works. (Verify the error message above.)
    Assert.assertEquals(parameters(4, 3, 16_973_333, 7_599_999), compute(8_000_000_000L, 4, 32, 107));
  }

  @Test
  public void test_oneWorkerInJvm_negativeMemory()
  {
    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(-100, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(-100, 1, 32), e.getFault());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerMemoryParameters.class).usingGetClass().verify();
  }

  private static WorkerMemoryParameters parameters(
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final long appenderatorMemory,
      final long broadcastJoinMemory
  )
  {
    return new WorkerMemoryParameters(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        appenderatorMemory,
        broadcastJoinMemory
    );
  }

  private static WorkerMemoryParameters compute(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers
  )
  {
    return WorkerMemoryParameters.compute(
        maxMemoryInJvm,
        numWorkersInJvm,
        numProcessingThreadsInJvm,
        numInputWorkers
    );
  }
}
