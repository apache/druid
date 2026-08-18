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

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.NoSuchElementException;

public class DelegateOrMinKeyCollectorTest
{
  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING)), 0);
  private final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
  private final Comparator<RowKey> comparator = clusterBy.keyComparator(signature);

  @Test
  public void testEmpty()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy, signature)
        ).newKeyCollector();

    Assertions.assertTrue(collector.getDelegate().isPresent());
    Assertions.assertTrue(collector.isEmpty());
    Assertions.assertThrows(NoSuchElementException.class, collector::minKey);
    Assertions.assertEquals(0, collector.estimatedRetainedBytes(), 0);
    Assertions.assertEquals(0, collector.estimatedTotalWeight());
    MatcherAssert.assertThat(collector.getDelegate().get(), CoreMatchers.instanceOf(QuantilesSketchKeyCollector.class));
  }

  @Test
  public void testDelegateAndMinKeyNotNullThrowsException()
  {
    Assertions.assertThrows(ISE.class, () -> {
      ClusterBy clusterBy = ClusterBy.none();
      new DelegateOrMinKeyCollector<>(
          clusterBy.keyComparator(RowSignature.empty()),
          QuantilesSketchKeyCollectorFactory.create(clusterBy, RowSignature.empty()).newKeyCollector(),
          RowKey.empty()
      );
    });
  }

  @Test
  public void testAdd()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy, signature)
        ).newKeyCollector();

    RowKey key = createKey(1L);
    collector.add(key, 1);

    Assertions.assertTrue(collector.getDelegate().isPresent());
    Assertions.assertFalse(collector.isEmpty());
    Assertions.assertEquals(key, collector.minKey());
    Assertions.assertEquals(key.estimatedObjectSizeBytes(), collector.estimatedRetainedBytes(), 0);
    Assertions.assertEquals(1, collector.estimatedTotalWeight());
  }

  @Test
  public void testDownSampleSingleKey()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy, signature)
        ).newKeyCollector();

    RowKey key = createKey(1L);

    collector.add(key, 1);
    Assertions.assertTrue(collector.downSample());

    Assertions.assertTrue(collector.getDelegate().isPresent());
    Assertions.assertFalse(collector.isEmpty());
    Assertions.assertEquals(key, collector.minKey());
    Assertions.assertEquals(key.estimatedObjectSizeBytes(), collector.estimatedRetainedBytes(), 0);
    Assertions.assertEquals(1, collector.estimatedTotalWeight());

    // Should not have actually downsampled, because the quantiles-based collector does nothing when
    // downsampling on a single key.
    Assertions.assertEquals(
        QuantilesSketchKeyCollectorFactory.SKETCH_INITIAL_K,
        collector.getDelegate().get().getSketch().getK()
    );
  }

  @Test
  public void testDownSampleTwoKeys()
  {
    final DelegateOrMinKeyCollector<QuantilesSketchKeyCollector> collector =
        new DelegateOrMinKeyCollectorFactory<>(
            comparator,
            QuantilesSketchKeyCollectorFactory.create(clusterBy, signature)
        ).newKeyCollector();

    RowKey key = createKey(1L);
    collector.add(key, 1);
    collector.add(key, 1);
    int expectedRetainedBytes = 2 * key.estimatedObjectSizeBytes();

    Assertions.assertTrue(collector.getDelegate().isPresent());
    Assertions.assertFalse(collector.isEmpty());
    Assertions.assertEquals(createKey(1L), collector.minKey());
    Assertions.assertEquals(expectedRetainedBytes, collector.estimatedRetainedBytes(), 0);
    Assertions.assertEquals(2, collector.estimatedTotalWeight());

    while (collector.getDelegate().isPresent()) {
      Assertions.assertTrue(collector.downSample());
    }
    expectedRetainedBytes = key.estimatedObjectSizeBytes();

    Assertions.assertFalse(collector.getDelegate().isPresent());
    Assertions.assertFalse(collector.isEmpty());
    Assertions.assertEquals(createKey(1L), collector.minKey());
    Assertions.assertEquals(expectedRetainedBytes, collector.estimatedRetainedBytes(), 0);
    Assertions.assertEquals(1, collector.estimatedTotalWeight());
  }

  private RowKey createKey(final Object... objects)
  {
    return KeyTestUtils.createKey(
        KeyTestUtils.createKeySignature(clusterBy.getColumns(), signature),
        FrameType.latestRowBased(),
        objects
    );
  }
}
