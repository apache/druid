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

package org.apache.druid.query.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.junit.Assert;
import org.junit.Test;

public class RowFilterPolicyTest
{
  private static final RowFilterPolicy SIMPLE_ROW_POLICY = RowFilterPolicy.from(new EqualityFilter(
      "col0",
      ColumnType.STRING,
      "val0",
      null
  ));

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(RowFilterPolicy.class)
                  .usingGetClass()
                  .withNonnullFields(new String[]{"rowFilter"})
                  .verify();
  }

  @Test
  public void test_deserialize_fromString() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Policy deserialized = jsonMapper.readValue(
        "{\"type\":\"row\",\"rowFilter\":{\"type\":\"equals\",\"column\":\"col0\",\"matchValueType\":\"STRING\",\"matchValue\":\"val0\"}}\n",
        Policy.class
    );
    Assert.assertEquals(SIMPLE_ROW_POLICY, deserialized);
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Policy deserialized = jsonMapper.readValue(jsonMapper.writeValueAsString(SIMPLE_ROW_POLICY), Policy.class);
    Assert.assertEquals(SIMPLE_ROW_POLICY, deserialized);
  }

  @Test
  public void testVisit()
  {
    DimFilter policyFilter = new EqualityFilter("col", ColumnType.STRING, "val", null);
    final RowFilterPolicy policy = RowFilterPolicy.from(policyFilter);

    Assert.assertEquals(policyFilter, policy.visit(CursorBuildSpec.FULL_SCAN).getFilter());
  }

  @Test
  public void testVisit_combineFilters()
  {
    Filter filter = new EqualityFilter("col0", ColumnType.STRING, "val0", null);
    CursorBuildSpec spec = CursorBuildSpec.builder()
                                          .setFilter(filter)
                                          .setPhysicalColumns(filter.getRequiredColumns())
                                          .build();

    DimFilter policyFilter = new EqualityFilter("col", ColumnType.STRING, "val", null);
    final RowFilterPolicy policy = RowFilterPolicy.from(policyFilter);

    Filter expected = new AndFilter(ImmutableList.of(policyFilter.toFilter(), filter));
    final CursorBuildSpec transformed = policy.visit(spec);
    Assert.assertEquals(expected, transformed.getFilter());
    Assert.assertEquals(ImmutableSet.of("col", "col0"), transformed.getPhysicalColumns());
  }
}
