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
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class RestrictAllTablesPolicyEnforcerTest
{
  @Test
  public void test_serialize() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    String expected = "{\"type\":\"restrictAllTables\",\"allowedPolicies\":[]}";
    Assert.assertEquals(expected, jsonMapper.writeValueAsString(policyEnforcer));
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    PolicyEnforcer deserialized = jsonMapper.readValue(
        jsonMapper.writeValueAsString(policyEnforcer),
        PolicyEnforcer.class
    );
    Assert.assertEquals(policyEnforcer, deserialized);
  }

  @Test
  public void test_validate() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("some-col", null));
    TableDataSource table = TableDataSource.create("table");
    RestrictedDataSource restricted = RestrictedDataSource.create(table, policy);

    Assert.assertFalse(policyEnforcer.validate(table));
    Assert.assertTrue(policyEnforcer.validate(restricted));
  }

  @Test
  public void test_validate_withAllowedPolicies() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(
        "NoRestrictionPolicy"));
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("some-col", null));
    TableDataSource table = TableDataSource.create("table");
    RestrictedDataSource restricted = RestrictedDataSource.create(table, policy);

    Assert.assertFalse(policyEnforcer.validate(table));
    Assert.assertFalse(policyEnforcer.validate(restricted));
  }
}
