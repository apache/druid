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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.hll.HLLCV0;
import io.druid.hll.HyperLogLogCollector;
import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.Random;

public class HyperUniquesAggregatorFactoryTest
{
  final static HyperUniquesAggregatorFactory aggregatorFactory = new HyperUniquesAggregatorFactory(
      "hyperUnique",
      "uniques"
  );
  final static String V0_BASE64 = "AAYbEyQwFyQVASMCVFEQQgEQIxIhM4ISAQMhUkICEDFDIBMhMgFQFAFAMjAAEhEREyVAEiUBAhIjISATMCECMiERIRIiVRFRAyIAEgFCQSMEJAITATAAEAMQgCEBEjQiAyUTAyEQASJyAGURAAISAwISATETQhAREBYDIVIlFTASAzJgERIgRCcmUyAwNAMyEJMjIhQXQhEWECABQDETATEREjIRAgEyIiMxMBQiAkBBMDYAMEQQACMzMhIkMTQSkYIRABIBADMBAhIEISAENkEBQDAxETMAIEEwEzQiQSEVQSFBBAQDICIiAVIAMTAQIQYBIRABADMDEzEAQSMkEiAYFBAQI0AmECEyQSARRTIVMhEkMiKAMCUBxUghAkIBI3EmMAQiACEAJDJCAAADOzESEDBCRjMgEUQQETQwEWIhA6MlAiAAZDI1AgEIIDUyFDIHMQEEAwIRBRABBStCZCQhAgJSMQIiQEEURTBmM1MxACIAETGhMgQnBRICNiIREyIUNAEAAkABAwQSEBJBIhIhIRERAiIRACUhEUAVMkQGEVMjECYjACBwEQQSIRIgAAEyExQUFSEAIBJCIDIDYTAgMiNBIUADUiETADMoFEADETMCIwUEQkIAESMSIzIABDERIXEhIiACQgUSEgJiQCAUARIRAREDQiEUAkQgAgQiIEAzIxRCARIgBAAVAzMAECEwE0Qh8gAAASEhEiAiMhUxcRImIVABATYyUBAwIoE1QhRDIiYBIBEBEiQSQyERAAADMAARAEACFYUwQSQBIRIgURITARFSEzEHEBACOTMREBIAMjIgEhU0cxEQIRIhIi1wEgMRUBEgMQIRAnAVASURMHQBAiEyBSAAEBQTAWQ5EQA0IUMSISAUEiASIjIhMhMFJBBSEjEAECEwACASEQFBAjARITEQIgYTEKEAeAAiMkEyARowARFBAicRISIBIxAQAgEBARMCIRQgMSIVIAkjMxIAIEMyADASMgFRIjEyKjEjBBIEQCUAARYBEQMxMCIBACNCACRCMlEzUUAAUDM1MhAjEgAxAAISAVFQECAhQAMBMhEzEgASNxAhFRIxECMRJBQAERAToBgQMhJSRQFAEhAwMiIhMQAwAgQiBQJiIGMQQhEiQxR1MiAjIAIEEiAkARECEzQlMjECIRATBgIhEBQAIQAEATEjBCMwAgMBMhAhIyFBIxQAARI1AAEABCIDFBIRUzMBIgAgEiARQCASMQQDQCFBAQAUJwMUElAyIAIRBSIRITICEAIxMAEUBEYTcBMBEEIxMREwIRIDAGIAEgYxBAEANCAhBAI2UhIiIgIRABIEVRAwNEIQERQgEFMhFCQSIAEhQDMTEQMiAjJyEQ==";

  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testDeserializeV0() throws Exception
  {
    Object v0 = aggregatorFactory.deserialize(V0_BASE64);
    Assert.assertEquals("deserialized value is HLLCV0", HLLCV0.class, v0.getClass());
  }

  @Test
  public void testCompare1() throws Exception
  {
    HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
    HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
    collector1.add(fn.hashLong(0).asBytes());
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 100; i = i + 2) {
      collector1.add(fn.hashLong(i).asBytes());
      collector2.add(fn.hashLong(i + 1).asBytes());
      Assert.assertEquals(1, comparator.compare(collector1, collector2));
      Assert.assertEquals(1, Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()));
    }
  }

  @Test
  public void testCompare2() throws Exception
  {
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 1000; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(50);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 1 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
              Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
              comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 100; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(500);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 2 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
              Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
              comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 10; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(100000);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 20000 + rand.nextInt(100000);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
              Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
              comparator.compare(collector1, collector2)
      );
    }
  }

  @Test
  public void testCompareToShouldBehaveConsistentlyWithEstimatedCardinalitiesEvenInToughCases() throws Exception
  {
    // given
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();

    for (int i = 0; i < 1000; ++i) {
      // given
      HyperLogLogCollector leftCollector = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(9000) + 5000;
      for (int l = 0; l < j; ++l) {
        leftCollector.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector rightCollector = HyperLogLogCollector.makeLatestCollector();
      int k = rand.nextInt(9000) + 5000;
      for (int l = 0; l < k; ++l) {
        rightCollector.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      // when
      final int orderedByCardinality = Double.compare(leftCollector.estimateCardinality(),
              rightCollector.estimateCardinality());
      final int orderedByComparator = comparator.compare(leftCollector, rightCollector);

      // then, assert hyperloglog comparator behaves consistently with estimated cardinalities
      Assert.assertEquals(
              StringUtils.format("orderedByComparator=%d, orderedByCardinality=%d,\n" +
                                 "Left={cardinality=%f, hll=%s},\n" +
                                 "Right={cardinality=%f, hll=%s},\n", orderedByComparator, orderedByCardinality,
                                 leftCollector.estimateCardinality(), leftCollector,
                                 rightCollector.estimateCardinality(), rightCollector),
              orderedByCardinality,
              orderedByComparator
      );
    }
  }
}
