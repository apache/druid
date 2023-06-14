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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ForeverLoadRuleTest
{
  @Test
  public void testSerdeNullTieredReplicants() throws Exception
  {
    ForeverLoadRule rule = new ForeverLoadRule(
        null
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule.getTieredReplicants(), ((ForeverLoadRule) reread).getTieredReplicants());
    Assert.assertEquals(ImmutableMap.of(), rule.getTieredReplicants());
  }

  @Test(expected = IAE.class)
  public void testCreatingNegativeTieredReplicants()
  {
    ForeverLoadRule foreverLoadRule = new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, -1));
  }

  @Test
  public void testEmptyTieredReplicants() throws Exception
  {
    ForeverLoadRule rule = new ForeverLoadRule(
        ImmutableMap.of()
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    LoadRule reread = (LoadRule) jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);
    Assert.assertEquals(ImmutableMap.of(), reread.getTieredReplicants());
  }

  @Test(expected = IAE.class)
  public void testEmptyReplicantValue() throws Exception
  {
    // Immutable map does not allow null values
    Map<String, Integer> tieredReplicants = new HashMap<>();
    tieredReplicants.put("tier", null);
    ForeverLoadRule rule = new ForeverLoadRule(
        tieredReplicants
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);
  }
}
