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

package org.apache.druid.emitter.ganglia;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class DimensionConverterTest
{
  @Test
  public void testConvert() throws Exception
  {
    DimensionConverter dimensionConverter = new DimensionConverter();
    String service = "127.0.0.1:8090";
    String metric = "jvm/gc/count";


    Map<String, Object> userDims = new HashMap<>();
    userDims.put("gcName", "yong");
    ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
    nameBuilder.add(service);
    nameBuilder.add(metric);
    SortedSet<String> dimensions = new TreeSet<>();
    dimensions.add("gcName");
    GangliaMetric ganglia = new GangliaMetric(dimensions);
    Map<String, GangliaMetric> map = new HashMap<>();
    GangliaMetric gangliaMetric = dimensionConverter.addFilteredUserDims(service, metric, userDims, nameBuilder, map);

    Assert.assertNull(gangliaMetric);

    map.put("jvm/gc/count", ganglia);
    gangliaMetric = dimensionConverter.addFilteredUserDims(service, metric, userDims, nameBuilder, map);
    Assert.assertEquals("gcName", gangliaMetric.dimensions.first());

    //String re = "druid.broker.jvm.gc.count.[young].[cms]".replaceAll("[^A-Za-z0-9:_.-]+", "");
    //System.out.println(re);
  }
}
