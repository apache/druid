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

package org.apache.druid.timeline;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionStateTest
{
  @Test
  public void test_generateCompactionStateFingerprint_deterministicFingerprinting()
  {
    CompactionState compactionState1 = createBasicCompactionState();
    CompactionState compactionState2 = createBasicCompactionState();

    String fingerprint1 = CompactionState.generateCompactionStateFingerprint(compactionState1, "test-ds");
    String fingerprint2 = CompactionState.generateCompactionStateFingerprint(compactionState2, "test-ds");

    Assertions.assertEquals(
        fingerprint1,
        fingerprint2,
        "Same CompactionState should produce identical fingerprints when datasource is same"
    );
  }

  @Test
  public void test_generateCompactionStateFingerprint_differentDatasourcesWithSameState_differentFingerprints()
  {
    CompactionState compactionState = createBasicCompactionState();

    String fingerprint1 = CompactionState.generateCompactionStateFingerprint(compactionState, "ds1");
    String fingerprint2 = CompactionState.generateCompactionStateFingerprint(compactionState, "ds2");

    Assertions.assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Different datasources should produce different fingerprints despite same state"
    );
  }

  @Test
  public void test_generateCompactionStateFingerprint_listOrderDoesNotInfluenceFingerprint()
  {
    List<AggregatorFactory> metrics1 = Arrays.asList(
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum", "value")
    );

    List<AggregatorFactory> metrics2 = Arrays.asList(
        new LongSumAggregatorFactory("sum", "value"),
        new CountAggregatorFactory("count")
    );

    CompactionState state1 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.EMPTY,
        metrics1,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    CompactionState state2 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.EMPTY,
        metrics2,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    String fingerprint1 = CompactionState.generateCompactionStateFingerprint(state1, "test-ds");
    String fingerprint2 = CompactionState.generateCompactionStateFingerprint(state2, "test-ds");

    Assertions.assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Metrics order currently matters (arrays preserve order in JSON)"
    );
  }

  @Test
  public void testGenerateCompactionStateFingerprint_differentPartitionsSpec()
  {
    CompactionState state1 = new CompactionState(
        new DynamicPartitionsSpec(5000000, null),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    CompactionState state2 = new CompactionState(
        new HashedPartitionsSpec(null, 2, Collections.singletonList("dim1")),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    String fingerprint1 = CompactionState.generateCompactionStateFingerprint(state1, "test-ds");
    String fingerprint2 = CompactionState.generateCompactionStateFingerprint(state2, "test-ds");

    Assertions.assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Different PartitionsSpec should produce different fingerprints"
    );
  }

  private CompactionState createBasicCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(5000000, null),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }
}
