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

package org.apache.druid.server.system;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SystemTablePushdownFilterTest
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("task_id", "id"),
      new SystemTablePushdownFilter("created_time", "created_date")
  );

  @Test
  public void testExtractsSupportedFilterWithMappedColumn()
  {
    final List<DimFilter> filters = extract(
        new EqualityFilter("task_id", ColumnType.STRING, "task-a", null)
    );

    Assertions.assertEquals(
        List.of(new EqualityFilter("id", ColumnType.STRING, "task-a", null)),
        filters
    );
  }

  @Test
  public void testExtractsSameColumnOrWithMappedColumn()
  {
    final List<DimFilter> filters = extract(
        new OrDimFilter(
            new SelectorDimFilter("task_id", "task-a", null),
            new SelectorDimFilter("task_id", "task-b", null)
        )
    );

    final OrDimFilter filter = Assertions.assertInstanceOf(OrDimFilter.class, filters.get(0));
    Assertions.assertEquals("id", ((SelectorDimFilter) filter.getFields().get(0)).getDimension());
    Assertions.assertEquals("id", ((SelectorDimFilter) filter.getFields().get(1)).getDimension());
  }

  @Test
  public void testExtractsLexicographicRangeWithMappedColumn()
  {
    final List<DimFilter> boundFilters = extract(
        new BoundDimFilter(
            "created_time",
            "2026-01-01",
            "2026-02-01",
            false,
            true,
            null,
            null,
            StringComparators.LEXICOGRAPHIC
        )
    );
    final List<DimFilter> rangeFilters = extract(
        new RangeFilter("task_id", ColumnType.STRING, "a", "z", false, true, null)
    );

    Assertions.assertEquals("created_date", ((BoundDimFilter) boundFilters.get(0)).getDimension());
    Assertions.assertEquals("id", ((RangeFilter) rangeFilters.get(0)).getColumn());
  }

  @Test
  public void testDoesNotExtractUnsupportedFilterShapeOrColumn()
  {
    Assertions.assertTrue(extract(new SelectorDimFilter("datasource", "wikipedia", null)).isEmpty());
  }

  @Test
  public void testExtractsLikeAndNotWithMappedColumn()
  {
    final List<DimFilter> likeFilters = extract(new LikeDimFilter("task_id", "%task%", null, null));
    final List<DimFilter> notFilters = extract(
        new NotDimFilter(new SelectorDimFilter("task_id", "task-a", null))
    );

    Assertions.assertEquals("id", ((LikeDimFilter) likeFilters.get(0)).getDimension());
    final NotDimFilter notFilter = (NotDimFilter) notFilters.get(0);
    Assertions.assertEquals("id", ((SelectorDimFilter) notFilter.getField()).getDimension());
  }

  private static List<DimFilter> extract(final DimFilter filter)
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new TableDataSource("test"))
                                  .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                                  .filters(filter)
                                  .build();
    return SystemTablePushdownFilter.extract(query, PUSHDOWN_FILTERS);
  }
}
