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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumnBitmapDisablingIndexSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;


public class VcBitmapIndexDisablingSqlTest extends BaseCalciteQueryTest
{
  @Test
  public void testSqlPlannedQueryVirtualColumnsHaveNoIndexSupplierWhenDisabled()
  {
    final Map<String, Object> ctx = ImmutableMap.<String, Object>builder()
                                                .putAll(QUERY_CONTEXT_DEFAULT)
                                                .put(
                                                    QueryContexts.CTX_MAX_VIRTUAL_COLUMNS_FOR_BITMAP,
                                                    0
                                                )
                                                .build();

    final String sql = "SELECT countryName + '' AS v0 FROM wikipedia LIMIT 1";

    testBuilder()
        .queryContext(ctx)
        .sql(sql)
        .expectedResults((s, queryResults) -> {
          final List<Query<?>> planned = queryResults.recordedQueries;
          Assertions.assertFalse(planned.isEmpty());

          final QueryableIndex index = TestIndex.getMMappedWikipediaIndex();

          for (Query<?> q : planned) {
            final VirtualColumns vcs = q.getVirtualColumns();
            if (vcs == null || vcs.isEmpty()) {
              continue;
            }

            try (var closer = Closer.create()) {
              final var columnCache = new
                  ColumnCache(index, vcs, closer);
              final var selector = new VirtualColumnBitmapDisablingIndexSelector(columnCache, vcs);

              Assertions.assertSame(NoIndexesColumnIndexSupplier.getInstance(), selector.getIndexSupplier("v0"));
              return;
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          fail("SQL did not plan any query with virtualColumns");
        })
        .run();
  }
}
