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


package org.apache.druid.server.broker;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class QueryConfigSnapshotTest
{
  @Test
  void testNullQueryContextValueFallsBackToDefault()
  {
    final Map<String, Object> context = new HashMap<>();
    context.put("key", null);
    context.put("key2", "user-provided-value");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("foo")
                                        .intervals("2000/3000")
                                        .granularity(Granularities.ALL)
                                        .context(context)
                                        .build();

    final QueryConfigSnapshot snapshot =
        new QueryConfigSnapshot(Map.of("key", "default", "key2", "default2"), null);
    Assertions.assertEquals(
        Map.of("key", "default", "key2", "user-provided-value"),
        snapshot.resolveContext(query, Set.of("key", "key2"))
    );
  }
}
