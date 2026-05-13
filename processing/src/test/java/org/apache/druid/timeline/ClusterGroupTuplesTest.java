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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class ClusterGroupTuplesTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final VirtualColumns VIRTUAL_COLUMNS = VirtualColumns.create(
      new ExpressionVirtualColumn(
          "tenant_lower",
          "lower(tenant)",
          ColumnType.STRING,
          TestExprMacroTable.INSTANCE
      )
  );

  private static RowSignature tenantRegion()
  {
    return RowSignature.builder()
                       .add("tenant", ColumnType.STRING)
                       .add("region", ColumnType.STRING)
                       .build();
  }

  private static RowSignature tenantPriority()
  {
    return RowSignature.builder()
                       .add("tenant", ColumnType.STRING)
                       .add("priority", ColumnType.LONG)
                       .build();
  }

  @Test
  void testConstructorRejectsNullClusteringColumns()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(null, List.of(List.of("acme", "us-east-1")))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("clusteringColumns must not be null or empty")
    );
  }

  @Test
  void testConstructorRejectsEmptyClusteringColumns()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(RowSignature.empty(), List.of())
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("clusteringColumns must not be null or empty")
    );
  }

  @Test
  void testConstructorAllowsEmptyTuples()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(tenantRegion(), List.of());
    Assertions.assertTrue(groups.getTuples().isEmpty());
  }

  @Test
  void testConstructorAllowsNullTuplesList()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(tenantRegion(), null);
    Assertions.assertTrue(groups.getTuples().isEmpty());
  }

  @Test
  void testConstructorRejectsTupleLengthMismatch()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(tenantRegion(), List.of(List.of("acme")))
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("tuple[0] has size [1] but clusteringColumns size is [2]")
    );
  }

  @Test
  void testConstructorRejectsNullTuple()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(tenantRegion(), Arrays.asList(Arrays.asList("acme", "us-east-1"), null))
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("tuple[1] has size [null] but clusteringColumns size is [2]")
    );
  }

  @Test
  void testConstructorRejectsUntypedClusteringColumn()
  {
    final RowSignature untyped = RowSignature.builder().add("tenant", null).build();
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(untyped, List.of(List.of("acme")))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("clusteringColumn[tenant] has no declared type")
    );
  }

  @Test
  void testConstructorRejectsUnsupportedColumnType()
  {
    final RowSignature arraySig = RowSignature.builder().add("arr", ColumnType.STRING_ARRAY).build();
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(arraySig, List.of(List.of(List.of("a"))))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("Unsupported clustering column type")
    );
  }

  @Test
  void testNullsAllowedAtAnyTuplePosition()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(
            Arrays.asList(null, "us-east-1"),
            Arrays.asList("acme", null),
            Arrays.asList(null, null)
        )
    );
    Assertions.assertEquals(3, groups.getTuples().size());
    Assertions.assertNull(groups.getTuples().get(0).get(0));
    Assertions.assertNull(groups.getTuples().get(1).get(1));
    Assertions.assertNull(groups.getTuples().get(2).get(0));
    Assertions.assertNull(groups.getTuples().get(2).get(1));
  }

  @Test
  void testEqualsAndHashCodeNullSafe()
  {
    final ClusterGroupTuples a = new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(Arrays.asList("acme", null), Arrays.asList(null, "us-east-1"))
    );
    final ClusterGroupTuples b = new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(Arrays.asList("acme", null), Arrays.asList(null, "us-east-1"))
    );
    Assertions.assertEquals(a, b);
    Assertions.assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void testCoercionIntegerToLong()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantPriority(),
        List.of(List.of("acme", Integer.valueOf(5)))
    );
    Assertions.assertEquals(Long.class, groups.getTuples().get(0).get(1).getClass());
    Assertions.assertEquals(5L, groups.getTuples().get(0).get(1));
  }

  @Test
  void testCoercionStringRejectedForLong()
  {
    // Coercion is intentionally narrow: only Number-family inputs are normalized. A String numeric value is rejected
    // rather than parsed, so operator typos don't silently broaden the matched set in future rule consumers.
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(tenantPriority(), List.of(List.of("acme", "42")))
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("Cannot coerce value [42] of type [java.lang.String] for column [priority] to LONG")
    );
  }

  @Test
  void testCoercionDoubleToFloat()
  {
    final RowSignature sig = RowSignature.builder().add("temp", ColumnType.FLOAT).build();
    final ClusterGroupTuples groups = new ClusterGroupTuples(sig, List.of(List.of((Object) Double.valueOf(98.6))));
    Assertions.assertEquals(Float.class, groups.getTuples().get(0).get(0).getClass());
    Assertions.assertEquals(98.6f, (Float) groups.getTuples().get(0).get(0), 0.0001f);
  }

  @Test
  void testCoercionStringRejectedForDouble()
  {
    final RowSignature sig = RowSignature.builder().add("v", ColumnType.DOUBLE).build();
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(sig, List.of(List.of("3.14")))
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("Cannot coerce value [3.14] of type [java.lang.String] for column [v] to DOUBLE")
    );
  }

  @Test
  void testCoercionBooleanRejectedForLong()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ClusterGroupTuples(tenantPriority(), List.of(List.of("acme", (Object) Boolean.TRUE)))
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageContains("Cannot coerce value [true] of type [java.lang.Boolean] for column [priority] to LONG")
    );
  }

  @Test
  void testCoercionAcceptsAnyTypeForString()
  {
    final RowSignature sig = RowSignature.builder().add("v", ColumnType.STRING).build();
    final ClusterGroupTuples groups = new ClusterGroupTuples(sig, List.of(List.of((Object) Long.valueOf(7))));
    Assertions.assertEquals("7", groups.getTuples().get(0).get(0));
  }

  @Test
  void testJsonRoundTripPreservesCoercedTypes() throws Exception
  {
    // Both small Integer (Jackson default) and large Long pass through coercion to canonical Long.
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantPriority(),
        List.of(List.of("acme", (Object) 5), List.of("globex", (Object) 5_000_000_000L))
    );
    final String json = MAPPER.writeValueAsString(groups);
    final ClusterGroupTuples back = MAPPER.readValue(json, ClusterGroupTuples.class);
    Assertions.assertEquals(groups, back);
    // Round-tripped tuples must end up with the same canonical types as the in-memory original.
    Assertions.assertEquals(Long.class, back.getTuples().get(0).get(1).getClass());
    Assertions.assertEquals(Long.class, back.getTuples().get(1).get(1).getClass());
  }

  @Test
  void testTuplesAreImmutable()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("acme", "us-east-1"))
    );
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> groups.getTuples().add(List.of("globex", "us-east-1"))
    );
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> groups.getTuples().get(0).set(0, "hijacked")
    );
  }

  @Test
  void testVirtualColumnsDefaultEmpty()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(tenantRegion(), List.of());
    Assertions.assertSame(VirtualColumns.EMPTY, groups.getVirtualColumns());
  }

  @Test
  void testVirtualColumnsAreStored()
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        RowSignature.builder().add("tenant_lower", ColumnType.STRING).build(),
        List.of(List.of("acme")),
        VIRTUAL_COLUMNS
    );
    Assertions.assertNotNull(groups.getVirtualColumns().getVirtualColumn("tenant_lower"));
  }

  @Test
  void testVirtualColumnsJsonRoundTrip() throws Exception
  {
    final ClusterGroupTuples original = new ClusterGroupTuples(
        RowSignature.builder().add("tenant_lower", ColumnType.STRING).build(),
        List.of(List.of("acme")),
        VIRTUAL_COLUMNS
    );
    // Round-trip needs an injectable ExprMacroTable for ExpressionVirtualColumn deserialization.
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
    );
    final String json = mapper.writeValueAsString(original);
    Assertions.assertTrue(json.contains("\"virtualColumns\""), () -> "expected virtualColumns in JSON: " + json);
    final ClusterGroupTuples back = mapper.readValue(json, ClusterGroupTuples.class);
    Assertions.assertEquals(original, back);
  }

  @Test
  void testVirtualColumnsOmittedFromJsonWhenEmpty() throws Exception
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(tenantRegion(), List.of(List.of("acme", "us-east-1")));
    final String json = MAPPER.writeValueAsString(groups);
    Assertions.assertFalse(json.contains("virtualColumns"), () -> "did not expect virtualColumns in JSON: " + json);
  }

  @Test
  void testVirtualColumnInternerSharesAcrossInstances()
  {
    // Two ClusterGroupTuples built from independent (but equal) VC inputs should share their VC instances via the
    // shared interner on DataSegment, so identical clustering VCs dedupe across segments held in memory.
    final ClusterGroupTuples a = new ClusterGroupTuples(
        RowSignature.builder().add("tenant_lower", ColumnType.STRING).build(),
        List.of(List.of("acme")),
        VIRTUAL_COLUMNS
    );
    final ClusterGroupTuples b = new ClusterGroupTuples(
        RowSignature.builder().add("tenant_lower", ColumnType.STRING).build(),
        List.of(List.of("globex")),
        VIRTUAL_COLUMNS
    );
    Assertions.assertSame(
        a.getVirtualColumns().getVirtualColumns()[0],
        b.getVirtualColumns().getVirtualColumns()[0]
    );
  }
}
