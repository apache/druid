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
 
package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
 
 /**
  */
 public class IntervalLoadRuleTest
 {
   @Test
   public void testSerde() throws Exception
   {
     IntervalLoadRule rule = new IntervalLoadRule(
         new Interval("0/3000"),
         ImmutableMap.<String, Integer>of(DruidServer.DEFAULT_TIER, 2)
     );
 
     ObjectMapper jsonMapper = new DefaultObjectMapper();
     Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);
 
     Assert.assertEquals(rule, reread);
   }

   @Test
   public void testSerdeNullTieredReplicants() throws Exception
   {
     IntervalLoadRule rule = new IntervalLoadRule(
         new Interval("0/3000"), null
     );

     ObjectMapper jsonMapper = new DefaultObjectMapper();
     Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

     Assert.assertEquals(rule, reread);
     Assert.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), rule.getTieredReplicants());
   }

   @Test
   public void testMappingNullTieredReplicants() throws Exception
   {
     String inputJson = "    {\n"
                        + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                        + "      \"type\": \"loadByInterval\"\n"
                        + "    }";
     String expectedJson = "{\n"
                           + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                           + "      \"tieredReplicants\": {\n"
                           + "        \""+ DruidServer.DEFAULT_TIER +"\": "+ DruidServer.DEFAULT_NUM_REPLICANTS +"\n"
                           + "      },\n"
                           + "      \"type\": \"loadByInterval\"\n"
                           + "    }";
     ObjectMapper jsonMapper = new DefaultObjectMapper();
     IntervalLoadRule inputIntervalLoadRule = jsonMapper.readValue(inputJson, IntervalLoadRule.class);
     IntervalLoadRule expectedIntervalLoadRule = jsonMapper.readValue(expectedJson, IntervalLoadRule.class);
     Assert.assertEquals(expectedIntervalLoadRule, inputIntervalLoadRule);
   }
 }
