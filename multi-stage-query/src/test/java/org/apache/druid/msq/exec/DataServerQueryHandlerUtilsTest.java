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

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.RestrictedInputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DataServerQueryHandlerUtilsTest
{
  @Test
  public void testTransformDatasource()
  {
    DataSource ds = DataServerQueryHandlerUtils.transformDatasource(new InputNumberDataSource(1), 1, "foo");
    Assertions.assertEquals(ds, TableDataSource.create("foo"));
  }

  @Test
  public void testTransformRestrictedDatasource()
  {
    DataSource ds = DataServerQueryHandlerUtils.transformDatasource(
        new RestrictedInputNumberDataSource(
            1,
            NoRestrictionPolicy.instance()
        ),
        1,
        "foo"
    );
    Assertions.assertEquals(ds, RestrictedDataSource.create(TableDataSource.create("foo"), NoRestrictionPolicy.instance()));
  }

  @Test
  public void testTransformUnionKeepsArmForInputNumber()
  {
    final DimFilter filter = new EqualityFilter("dim1", ColumnType.STRING, "abc", null);
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("v0", "concat(\"dim2\", 'x')", ColumnType.STRING, ExprMacroTable.nil())
    );
    final RowFilterPolicy policy = RowFilterPolicy.from(new EqualityFilter("dim3", ColumnType.STRING, "a", null));

    final DataSource union = new UnionDataSource(
        List.of(
            FilteredDataSource.create(new InputNumberDataSource(0), filter),
            new ProjectDataSource(new InputNumberDataSource(1), virtualColumns),
            new RestrictedInputNumberDataSource(2, policy)
        )
    );

    Assertions.assertEquals(
        FilteredDataSource.create(TableDataSource.create("foo"), filter),
        DataServerQueryHandlerUtils.transformDatasource(union, 0, "foo")
    );
    Assertions.assertEquals(
        new ProjectDataSource(TableDataSource.create("bar"), virtualColumns),
        DataServerQueryHandlerUtils.transformDatasource(union, 1, "bar")
    );
    Assertions.assertEquals(
        RestrictedDataSource.create(TableDataSource.create("baz"), policy),
        DataServerQueryHandlerUtils.transformDatasource(union, 2, "baz")
    );
  }

  @Test
  public void testTransformUnionUnderLookupJoin()
  {
    final DataSource union = new UnionDataSource(
        List.of(new InputNumberDataSource(0), new InputNumberDataSource(1))
    );

    Assertions.assertEquals(
        makeLookupJoin(TableDataSource.create("bar")),
        DataServerQueryHandlerUtils.transformDatasource(makeLookupJoin(union), 1, "bar")
    );
  }

  @Test
  public void testTransformUnionUnderBroadcastJoin()
  {
    final DataSource union = new UnionDataSource(
        List.of(new InputNumberDataSource(0), new InputNumberDataSource(1))
    );
    final DataSource join = JoinDataSource.create(
        union,
        new InputNumberDataSource(2),
        "j.",
        "dim1 == \"j.k\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> DataServerQueryHandlerUtils.transformDatasource(join, 0, "foo")
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot handle stage with multiple sources"), e.getMessage());
  }

  @Test
  public void testTransformUnionWithoutInputNumber()
  {
    final DataSource union = new UnionDataSource(
        List.of(new InputNumberDataSource(0), new InputNumberDataSource(1))
    );

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> DataServerQueryHandlerUtils.transformDatasource(union, 2, "foo")
    );
    Assertions.assertTrue(e.getMessage().contains("Expected exactly one arm of union"), e.getMessage());
  }

  private static DataSource makeLookupJoin(final DataSource left)
  {
    return JoinDataSource.create(
        left,
        new LookupDataSource("lookyloo"),
        "j.",
        "dim1 == \"j.k\"",
        JoinType.LEFT,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
  }
}
