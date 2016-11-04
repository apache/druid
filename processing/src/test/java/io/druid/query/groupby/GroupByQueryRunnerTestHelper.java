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

package io.druid.query.groupby;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryRunnerTestHelper
{
  public static <T> Iterable<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {

    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }

  public static Row createExpectedRow(final String timestamp, Object... vals)
  {
    return createExpectedRow(new DateTime(timestamp), vals);
  }

  public static Row createExpectedRow(final DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = new DateTime(timestamp);
    return new MapBasedRow(ts, theVals);
  }

  public static List<Row> createExpectedRows(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    Preconditions.checkArgument(timeIndex >= 0);

    List<Row> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newHashMapWithExpectedSize(value.length);
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        }
      }
      expected.add(new MapBasedRow(new DateTime(value[timeIndex]), theVals));
    }
    return expected;
  }

}
