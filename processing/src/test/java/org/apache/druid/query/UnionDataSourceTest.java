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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UnionDataSourceTest
{
  private final UnionDataSource unionDataSource = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("foo"),
          new TableDataSource("bar")
      )
  );

  private final UnionDataSource unionDataSourceWithDuplicates = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("bar"),
          new TableDataSource("foo"),
          new TableDataSource("bar")
      )
  );

  private final UnionDataSource tableAndInlineUniondataSource = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("foo"),
          InlineDataSource.fromIterable(
              Collections.emptyList(),
              RowSignature.empty()
          )
      )
  );

  @Test
  public void test_constructor_empty()
  {
    IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        //noinspection ResultOfObjectAllocationIgnored
        () -> new UnionDataSource(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("'dataSources' must be non-null and non-empty for 'union'"));
  }

  @Test
  public void test_isTableBased()
  {
    Assertions.assertTrue(unionDataSource.isTableBased());
    Assertions.assertTrue(unionDataSourceWithDuplicates.isTableBased());

    Assertions.assertFalse(tableAndInlineUniondataSource.isTableBased());
  }

  @Test
  public void test_isTableBased_decoratedArms()
  {
    // Arms may decorate tables.
    final DataSource decoratedTable = new ProjectDataSource(
        FilteredDataSource.create(
            RestrictedDataSource.create(new TableDataSource("foo"), NoRestrictionPolicy.instance()),
            TrueDimFilter.instance()
        ),
        VirtualColumns.EMPTY
    );
    Assertions.assertTrue(new UnionDataSource(List.of(decoratedTable, new TableDataSource("bar"))).isTableBased());

    // But not inline data.
    final DataSource decoratedInline = FilteredDataSource.create(
        InlineDataSource.fromIterable(Collections.emptyList(), RowSignature.empty()),
        TrueDimFilter.instance()
    );
    Assertions.assertFalse(new UnionDataSource(List.of(decoratedInline, new TableDataSource("bar"))).isTableBased());

    // Nor a subquery, even one that reads tables, since it cannot be run as-is.
    final DataSource subquery =
        new QueryDataSource(Druids.newScanQueryBuilder().dataSource("foo").eternityInterval().build());
    Assertions.assertFalse(new UnionDataSource(List.of(subquery, new TableDataSource("bar"))).isTableBased());
  }

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSource.getTableNames());
  }

  @Test
  public void test_getTableNames_withDuplicates()
  {
    Assertions.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSourceWithDuplicates.getTableNames());
  }

  @Test
  public void test_getReservedColumns()
  {
    Assertions.assertEquals(Set.of(), unionDataSource.getReservedColumns());

    // Names reserved by any arm are reserved.
    Assertions.assertEquals(
        Set.of("v0", "dim1", "dim2"),
        new UnionDataSource(
            ImmutableList.of(
                new ProjectDataSource(
                    new TableDataSource("foo"),
                    VirtualColumns.create(new ExpressionVirtualColumn("v0", Parser.identifier("dim1"), null))
                ),
                FilteredDataSource.create(new TableDataSource("bar"), DimFilters.dimEquals("dim2", "x"))
            )
        ).getReservedColumns()
    );
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(
        ImmutableList.of(new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSource.getChildren()
    );
  }

  @Test
  public void test_getChildren_withDuplicates()
  {
    Assertions.assertEquals(
        ImmutableList.of(new TableDataSource("bar"), new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSourceWithDuplicates.getChildren()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertFalse(unionDataSource.isCacheable(true));
    Assertions.assertFalse(unionDataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertFalse(unionDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(unionDataSource.isProcessable());
  }

  @Test
  public void test_withChildren_empty()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> unionDataSource.withChildren(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("Expected [2] children, got [0]"));
  }

  @Test
  public void test_withChildren_sameNumber()
  {
    final List<DataSource> newDataSources = ImmutableList.of(
        new TableDataSource("baz"),
        new TableDataSource("qux")
    );

    //noinspection unchecked
    Assertions.assertEquals(
        new UnionDataSource(newDataSources),
        unionDataSource.withChildren(newDataSources)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(UnionDataSource.class).usingGetClass().withNonnullFields("dataSources").verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final UnionDataSource deserialized = (UnionDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(unionDataSource),
        DataSource.class
    );

    Assertions.assertEquals(unionDataSource, deserialized);
  }
}
