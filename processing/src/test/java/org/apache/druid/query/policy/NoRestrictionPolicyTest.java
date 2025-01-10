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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class NoRestrictionPolicyTest
{
  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(NoRestrictionPolicy.class).usingGetClass().verify();
  }

  @Test
  public void test_deserialize_fromString() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Policy deserialized = jsonMapper.readValue("{\"type\":\"noRestriction\"}", Policy.class);
    Assert.assertEquals(NoRestrictionPolicy.instance(), deserialized);
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    final NoRestrictionPolicy policy = NoRestrictionPolicy.instance();
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Policy deserialized = jsonMapper.readValue(jsonMapper.writeValueAsString(policy), Policy.class);
    Assert.assertEquals(policy, deserialized);
  }

  @Test
  public void testVisit()
  {
    final NoRestrictionPolicy policy = NoRestrictionPolicy.instance();
    Assert.assertEquals(CursorBuildSpec.FULL_SCAN, policy.visit(CursorBuildSpec.FULL_SCAN));
  }
}
