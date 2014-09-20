/*
  * Druid - a distributed column store.
  * Copyright (C) 2012, 2013  Metamarkets Group Inc.
  *
  * This program is free software; you can redistribute it and/or
  * modify it under the terms of the GNU General Public License
  * as published by the Free Software Foundation; either version 2
  * of the License, or (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program; if not, write to the Free Software
  * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
         ImmutableMap.<String, Integer>of(DruidServer.DEFAULT_TIER, 2),
         null,
         null
     );
 
     ObjectMapper jsonMapper = new DefaultObjectMapper();
     Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);
 
     Assert.assertEquals(rule, reread);
   }
 }