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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class PolicyConfigTest
{

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(PolicyConfig.class).usingGetClass().verify();
  }

  @Test
  public void test_deserialize_fromString() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    PolicyConfig deserialized = jsonMapper.readValue(
        "{\"tablePolicySecurityLevel\":\"2.0f\",\"allowedPolicies\":[\"NoRestrictionPolicy\",\"RowFilterPolicy\"]}",
        PolicyConfig.class
    );
    Assert.assertEquals(new PolicyConfig(
        PolicyConfig.TablePolicySecurityLevel.POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST,
        ImmutableList.of(NoRestrictionPolicy.class.getSimpleName(), RowFilterPolicy.class.getSimpleName())
    ), deserialized);
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    final PolicyConfig policyConfig = PolicyConfig.defaultInstance();
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    PolicyConfig deserialized = jsonMapper.readValue(jsonMapper.writeValueAsString(policyConfig), PolicyConfig.class);
    Assert.assertEquals(policyConfig, deserialized);
  }

  @Test
  public void test_policyMustBeCheckedAndExistOnAllTables() throws Exception
  {
    PolicyConfig defaultConfig = PolicyConfig.defaultInstance();
    PolicyConfig config = new PolicyConfig(
        PolicyConfig.TablePolicySecurityLevel.POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST,
        ImmutableList.of(NoRestrictionPolicy.class.getSimpleName())
    );

    Assert.assertFalse(defaultConfig.policyMustBeCheckedAndExistOnAllTables());
    Assert.assertTrue(config.policyMustBeCheckedAndExistOnAllTables());
  }

  @Test
  public void test_allowPolicy() throws Exception
  {
    PolicyConfig defaultConfig = PolicyConfig.defaultInstance();
    PolicyConfig config = new PolicyConfig(
        PolicyConfig.TablePolicySecurityLevel.POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST,
        ImmutableList.of(NoRestrictionPolicy.class.getSimpleName())
    );
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("some-col", null));

    Assert.assertTrue(defaultConfig.allowPolicy(policy));
    Assert.assertFalse(config.allowPolicy(policy));
    Assert.assertEquals(ImmutableList.of(NoRestrictionPolicy.class.getSimpleName()), config.getAllowedPolicies());
    Assert.assertEquals(ImmutableList.of(), defaultConfig.getAllowedPolicies());
  }
}
