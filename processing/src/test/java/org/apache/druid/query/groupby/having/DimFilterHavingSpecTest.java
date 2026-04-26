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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DimFilterHavingSpecTest
{
  @Test
  public void testSimple()
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "bar", null), null);
    havingSpec.setQuery(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setDimensions(DefaultDimensionSpec.of("foo"))
                    .setGranularity(Granularities.ALL)
                    .build()
    );

    Assertions.assertTrue(havingSpec.eval(ResultRow.of("bar")));
    Assertions.assertFalse(havingSpec.eval(ResultRow.of("baz")));
  }

  @Test
  public void testRowSignature()
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), null);
    havingSpec.setQuery(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("foo", "foo", ColumnType.LONG))
                    .build()
    );

    Assertions.assertTrue(havingSpec.eval(ResultRow.of(1L)));
    Assertions.assertFalse(havingSpec.eval(ResultRow.of(2L)));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  @Disabled("Doesn't always pass. The check in \"eval\" is best effort and not guaranteed to detect concurrent usage.")
  public void testConcurrentUsage() throws Exception
  {
    final ExecutorService exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "DimFilterHavingSpecTest-%d"));
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), null);
    final List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      final ResultRow row = ResultRow.of(String.valueOf(i));
      futures.add(
          exec.submit(
              () -> {
                havingSpec.setQuery(GroupByQuery.builder().setDimensions(DefaultDimensionSpec.of("foo")).build());
                while (!Thread.interrupted()) {
                  havingSpec.eval(row);
                }
              }
          )
      );
    }

    ExecutionException ex = Assertions.assertThrows(ExecutionException.class, () -> {
      for (Future<?> future : futures) {
        future.get();
      }
    });
    exec.shutdownNow();
    Assertions.assertInstanceOf(IllegalStateException.class, ex.getCause());
    Assertions.assertTrue(ex.getCause().getMessage().contains("concurrent 'eval' calls not permitted"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), false);
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    Assertions.assertEquals(
        havingSpec,
        objectMapper.readValue(objectMapper.writeValueAsBytes(havingSpec), HavingSpec.class)
    );
  }
}
