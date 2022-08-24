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
    Assert.assertEquals(parameters(1, 45, 373_000_000), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 14, 248_000_000), compute(1_000_000_000, 1, 2, 1));
    Assert.assertEquals(parameters(4, 3, 148_000_000), compute(1_000_000_000, 1, 4, 1));
    Assert.assertEquals(parameters(3, 2, 81_333_333), compute(1_000_000_000, 1, 8, 1));
    Assert.assertEquals(parameters(1, 4, 42_117_647), compute(1_000_000_000, 1, 16, 1));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(1_000_000_000, 1, 32), e.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 45, 174_000_000), compute(1_000_000_000, 1, 1, 200));
    Assert.assertEquals(parameters(2, 14, 49_000_000), compute(1_000_000_000, 1, 2, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 4, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 124), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 149, 999_000_000), compute(8_000_000_000L, 4, 1, 200));
    Assert.assertEquals(parameters(2, 61, 799_000_000), compute(8_000_000_000L, 4, 2, 200));
    Assert.assertEquals(parameters(4, 22, 549_000_000), compute(8_000_000_000L, 4, 4, 200));
    Assert.assertEquals(parameters(4, 14, 299_000_000), compute(8_000_000_000L, 4, 8, 200));
    Assert.assertEquals(parameters(4, 8, 99_000_000), compute(8_000_000_000L, 4, 16, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(8_000_000_000L, 4, 32, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 140), e.getFault());

    // Make sure 140 actually works. (Verify the error message above.)
    Assert.assertEquals(parameters(4, 4, 25_666_666), compute(8_000_000_000L, 4, 32, 140));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerMemoryParameters.class).usingGetClass().verify();
  }

  private static WorkerMemoryParameters parameters(
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final long bundleMemory
  )
  {
    return new WorkerMemoryParameters(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        (long) (bundleMemory * WorkerMemoryParameters.APPENDERATOR_MEMORY_FRACTION),
        (long) (bundleMemory * WorkerMemoryParameters.BROADCAST_JOIN_MEMORY_FRACTION)
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
