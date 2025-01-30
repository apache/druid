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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class DataSourceTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerialization() throws IOException
  {
    DataSource dataSource = new TableDataSource("somedatasource");
    String json = JSON_MAPPER.writeValueAsString(dataSource);
    DataSource serdeDataSource = JSON_MAPPER.readValue(json, DataSource.class);
    Assert.assertEquals(dataSource, serdeDataSource);
  }

  @Test
  public void testLegacyDataSource() throws IOException
  {
    DataSource dataSource = JSON_MAPPER.readValue("\"somedatasource\"", DataSource.class);
    Assert.assertEquals(new TableDataSource("somedatasource"), dataSource);
  }

  @Test
  public void testTableDataSource() throws IOException
  {
    DataSource dataSource = JSON_MAPPER.readValue(
        "{\"type\":\"table\", \"name\":\"somedatasource\"}",
        DataSource.class
    );
    Assert.assertEquals(new TableDataSource("somedatasource"), dataSource);
  }

  @Test
  public void testRestrictedDataSource() throws IOException
  {
    DataSource dataSource = JSON_MAPPER.readValue(
        "{\"type\":\"restrict\",\"base\":{\"type\":\"table\",\"name\":\"somedatasource\"},\"policy\":{\"type\":\"noRestriction\"}}\n",
        DataSource.class
    );

    Assert.assertEquals(
        RestrictedDataSource.create(TableDataSource.create("somedatasource"), NoRestrictionPolicy.instance()),
        dataSource
    );
  }

  @Test
  public void testQueryDataSource() throws IOException
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    String dataSourceJSON = "{\"type\":\"query\", \"query\":" + JSON_MAPPER.writeValueAsString(query) + "}";

    DataSource dataSource = JSON_MAPPER.readValue(dataSourceJSON, DataSource.class);
    Assert.assertEquals(new QueryDataSource(query), dataSource);
  }

  @Test
  public void testUnionDataSource() throws Exception
  {
    DataSource dataSource = JSON_MAPPER.readValue(
        "{\"type\":\"union\", \"dataSources\":[\"ds1\", \"ds2\"]}",
        DataSource.class
    );
    Assert.assertTrue(dataSource instanceof UnionDataSource);
    Assert.assertEquals(
        Lists.newArrayList(new TableDataSource("ds1"), new TableDataSource("ds2")),
        Lists.newArrayList(((UnionDataSource) dataSource).getDataSourcesAsTableDataSources())
    );
    Assert.assertEquals(
        ImmutableSet.of("ds1", "ds2"),
        dataSource.getTableNames()
    );

    final DataSource serde = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(dataSource), DataSource.class);
    Assert.assertEquals(dataSource, serde);
  }

  @Test
  public void testMapWithRestriction()
  {
    TableDataSource table1 = TableDataSource.create("table1");
    TableDataSource table2 = TableDataSource.create("table2");
    TableDataSource table3 = TableDataSource.create("table3");
    UnionDataSource unionDataSource = new UnionDataSource(Lists.newArrayList(table1, table2, table3));
    ImmutableMap<String, Optional<Policy>> restrictions = ImmutableMap.of(
        "table1",
        Optional.of(NoRestrictionPolicy.instance()),
        "table2",
        Optional.of(NoRestrictionPolicy.instance()),
        "table3",
        Optional.of(RowFilterPolicy.from(new NullFilter(
            "some-column",
            null
        )))
    );

    Assert.assertEquals(
        unionDataSource.withPolicies(restrictions),
        new UnionDataSource(Lists.newArrayList(
            RestrictedDataSource.create(
                table1,
                NoRestrictionPolicy.instance()
            ),
            RestrictedDataSource.create(
                table2,
                NoRestrictionPolicy.instance()
            ),
            RestrictedDataSource.create(
                table3,
                RowFilterPolicy.from(new NullFilter(
                    "some-column",
                    null
                ))
            )
        ))
    );
  }

  @Test
  public void testMapWithRestriction_onRestrictedDataSource_fromDruidSystem()
  {
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create("table1"),
        RowFilterPolicy.from(new NullFilter("some-column", null))
    );
    // The druid-system should get a NO_RESTRICTION policy attached on a table.
    ImmutableMap<String, Optional<Policy>> noRestrictionPolicy = ImmutableMap.of(
        "table1",
        Optional.of(NoRestrictionPolicy.instance())
    );

    Assert.assertEquals(restrictedDataSource, restrictedDataSource.withPolicies(noRestrictionPolicy));
  }

  @Test
  public void testMapWithRestriction_onRestrictedDataSource_alwaysThrows()
  {
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create("table1"),
        RowFilterPolicy.from(new NullFilter("random-column", null))
    );
    ImmutableMap<String, Optional<Policy>> anotherRestrictions = ImmutableMap.of(
        "table1",
        Optional.of(RowFilterPolicy.from(new NullFilter("some-column", null)))
    );
    ImmutableMap<String, Optional<Policy>> noPolicyFound = ImmutableMap.of("table1", Optional.empty());
    ImmutableMap<String, Optional<Policy>> policyWasNotChecked = ImmutableMap.of();

    ISE e = Assert.assertThrows(ISE.class, () -> restrictedDataSource.withPolicies(anotherRestrictions));
    Assert.assertEquals(
        "Multiple restrictions on table [table1]: policy [RowFilterPolicy{rowFilter=random-column IS NULL}] and policy [RowFilterPolicy{rowFilter=some-column IS NULL}]",
        e.getMessage()
    );

    ISE e2 = Assert.assertThrows(ISE.class, () -> restrictedDataSource.withPolicies(noPolicyFound));
    Assert.assertEquals(
        "No restriction found on table [table1], but had policy [RowFilterPolicy{rowFilter=random-column IS NULL}] before.",
        e2.getMessage()
    );
    ISE e3 = Assert.assertThrows(ISE.class, () -> restrictedDataSource.withPolicies(policyWasNotChecked));
    Assert.assertEquals("Missing policy check result for table [table1]", e3.getMessage());
  }
}
