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

package io.druid.query.select;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.spec.LegacySegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class SelectQuerySpecTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  {
    objectMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            SelectQueryConfig.class,
            new SelectQueryConfig(true)
        )
    );
  }

  @Test
  public void testSerializationLegacyString() throws Exception
  {
    String legacy =
        "{\"queryType\":\"select\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},"
        + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z\"]},"
        + "\"descending\":true,"
        + "\"filter\":null,"
        + "\"granularity\":{\"type\":\"all\"},"
        + "\"dimensions\":[\"market\",\"quality\"],"
        + "\"metrics\":[\"index\"],"
        + "\"virtualColumns\":null,"
        + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":3},"
        + "\"context\":null}";

    String current =
        "{\"queryType\":\"select\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},"
        + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z\"]},"
        + "\"descending\":true,"
        + "\"filter\":null,"
        + "\"granularity\":{\"type\":\"all\"},"
        + "\"dimensions\":"
        + "[{\"type\":\"default\",\"dimension\":\"market\",\"outputName\":\"market\",\"outputType\":\"STRING\"},"
        + "{\"type\":\"default\",\"dimension\":\"quality\",\"outputName\":\"quality\",\"outputType\":\"STRING\"}],"
        + "\"metrics\":[\"index\"],"
        + "\"virtualColumns\":[],"
        + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":3,\"fromNext\":true},"
        + "\"context\":null}";

    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new LegacySegmentSpec(new Interval("2011-01-12/2011-01-14")),
        true,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.<String>asList("market", "quality")),
        Arrays.<String>asList("index"),
        null,
        new PagingSpec(null, 3, null),
        null
    );

    String actual = objectMapper.writeValueAsString(query);
    Assert.assertEquals(current, actual);
    Assert.assertEquals(query, objectMapper.readValue(actual, SelectQuery.class));
    Assert.assertEquals(query, objectMapper.readValue(legacy, SelectQuery.class));
  }

  @Test
  public void testPagingSpecFromNext() throws Exception
  {
    String baseQueryJson =
        "{\"queryType\":\"select\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},"
        + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z\"]},"
        + "\"descending\":true,"
        + "\"filter\":null,"
        + "\"granularity\":{\"type\":\"all\"},"
        + "\"dimensions\":"
        + "[{\"type\":\"default\",\"dimension\":\"market\",\"outputName\":\"market\",\"outputType\":\"STRING\"},"
        + "{\"type\":\"default\",\"dimension\":\"quality\",\"outputName\":\"quality\",\"outputType\":\"STRING\"}],"
        + "\"metrics\":[\"index\"],"
        + "\"virtualColumns\":[],";

    String withNull =
        baseQueryJson
        + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":3,\"fromNext\":null},"
        + "\"context\":null}";

    String withFalse =
        baseQueryJson
        + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":3,\"fromNext\":false},"
        + "\"context\":null}";

    String withTrue =
        baseQueryJson
        + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":3,\"fromNext\":true},"
        + "\"context\":null}";

    SelectQuery queryWithNull = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new LegacySegmentSpec(new Interval("2011-01-12/2011-01-14")),
        true,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.<String>asList("market", "quality")),
        Arrays.<String>asList("index"),
        null,
        new PagingSpec(null, 3, null),
        null
    );

    SelectQuery queryWithFalse = queryWithNull.withPagingSpec(
        new PagingSpec(null, 3, false)
    );

    SelectQuery queryWithTrue = queryWithNull.withPagingSpec(
        new PagingSpec(null, 3, true)
    );

    String actualWithNull = objectMapper.writeValueAsString(queryWithNull);
    Assert.assertEquals(withTrue, actualWithNull);

    String actualWithFalse = objectMapper.writeValueAsString(queryWithFalse);
    Assert.assertEquals(withFalse, actualWithFalse);

    String actualWithTrue = objectMapper.writeValueAsString(queryWithTrue);
    Assert.assertEquals(withTrue, actualWithTrue);

    Assert.assertEquals(queryWithNull, objectMapper.readValue(actualWithNull, SelectQuery.class));
    Assert.assertEquals(queryWithFalse, objectMapper.readValue(actualWithFalse, SelectQuery.class));
    Assert.assertEquals(queryWithTrue, objectMapper.readValue(actualWithTrue, SelectQuery.class));
  }
}
