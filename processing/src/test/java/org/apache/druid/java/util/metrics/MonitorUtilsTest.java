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

package org.apache.druid.java.util.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MonitorUtilsTest
{
  @Test
  public void testAddDimensionsToBuilder()
  {
    ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    Map<String, String[]> dimensions = ImmutableMap.of(
        "dim1", new String[]{"value1"},
        "dim2", new String[]{"value2.1", "value2.2"}
    );

    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    Assert.assertEquals(builder.getDimension("dim1"), ImmutableList.of("value1"));
    Assert.assertEquals(builder.getDimension("dim2"), ImmutableList.of("value2.1", "value2.2"));
  }
}
