/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import junit.framework.Assert;
import org.joda.time.Interval;
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
 }
