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

package org.apache.druid.data.input.impl;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusteredValueGroupsBaseTableProjectionSpecTest extends InitializedNullHandlingTest
{
  private static ClusteredValueGroupsBaseTableProjectionSpec tenantSpec()
  {
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(new StringDimensionSchema("tenant"), new StringDimensionSchema("region"), new LongDimensionSchema("__time"))
        .clusteringColumns("tenant")
        .build();
  }

  @Test
  void testWithQueryGranularityAddsVirtualGranularityColumn()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec().withQueryGranularity(Granularities.HOUR);

    final VirtualColumn vc = spec.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertNotNull(vc);
    Assertions.assertEquals(Granularities.HOUR, Granularities.fromVirtualColumn(vc));

    // The rest of the spec is unchanged.
    Assertions.assertEquals(tenantSpec().getClusteringColumns(), spec.getClusteringColumns());
    Assertions.assertEquals(tenantSpec().getColumns(), spec.getColumns());
    Assertions.assertEquals(tenantSpec().getOrdering(), spec.getOrdering());
    Assertions.assertArrayEquals(tenantSpec().getMetrics(), spec.getMetrics());
  }

  @Test
  void testWithQueryGranularityNullAndNoneAreNoOps()
  {
    // Absent __virtualGranularity virtual column already means NONE, so null/NONE add nothing and return the same spec.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec();
    Assertions.assertSame(spec, spec.withQueryGranularity(null));
    Assertions.assertSame(spec, spec.withQueryGranularity(Granularities.NONE));
  }

  @Test
  void testWithQueryGranularityAllIsRejected()
  {
    // ALL would floor __time to a single constant (the interval start) for the whole segment, which clustered base
    // tables do not yet support, so it is rejected rather than silently ignored.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec();
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> spec.withQueryGranularity(Granularities.ALL)
    );
    Assertions.assertTrue(e.getMessage().contains("ALL"));
  }

  @Test
  void testGetQueryGranularityRoundTrips()
  {
    Assertions.assertEquals(
        Granularities.HOUR,
        tenantSpec().withQueryGranularity(Granularities.HOUR).getQueryGranularity()
    );
  }

  @Test
  void testGetQueryGranularityIsNoneWhenNoVirtualColumn()
  {
    Assertions.assertEquals(Granularities.NONE, tenantSpec().getQueryGranularity());
  }

  @Test
  void testHasEqualCompactionStateIgnoresQueryGranularity()
  {
    // Two specs that differ only in their query granularity are equivalent for compaction (query granularity is
    // compared by its own check), regardless of which granularity each carries.
    Assertions.assertTrue(tenantSpec().withQueryGranularity(Granularities.HOUR).hasEqualCompactionState(tenantSpec()));
    Assertions.assertTrue(tenantSpec().hasEqualCompactionState(tenantSpec().withQueryGranularity(Granularities.HOUR)));
    Assertions.assertTrue(
        tenantSpec().withQueryGranularity(Granularities.HOUR)
                    .hasEqualCompactionState(tenantSpec().withQueryGranularity(Granularities.DAY))
    );
  }

  @Test
  void testHasEqualCompactionStateComparesSchema()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec differentClustering =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(new StringDimensionSchema("tenant"), new StringDimensionSchema("region"), new LongDimensionSchema("__time"))
            .clusteringColumns("tenant", "region")
            .build();

    Assertions.assertFalse(tenantSpec().hasEqualCompactionState(differentClustering));
    Assertions.assertTrue(tenantSpec().hasEqualCompactionState(tenantSpec()));
  }

  @Test
  void testWithQueryGranularityIsIdempotentWhenAlreadyPresent()
  {
    // Once a query-granularity virtual column is present it is authoritative; a second call is a no-op and does not
    // double-add or change it (the compaction path attaches it up front, then MSQ generation calls this again).
    final ClusteredValueGroupsBaseTableProjectionSpec withGranularity =
        tenantSpec().withQueryGranularity(Granularities.HOUR);
    final ClusteredValueGroupsBaseTableProjectionSpec reapplied = withGranularity.withQueryGranularity(Granularities.DAY);

    Assertions.assertSame(withGranularity, reapplied);
    Assertions.assertEquals(
        Granularities.HOUR,
        Granularities.fromVirtualColumn(
            reapplied.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
        )
    );
  }
}
