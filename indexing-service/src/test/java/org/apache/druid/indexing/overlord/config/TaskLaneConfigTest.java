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

package org.apache.druid.indexing.overlord.config;


import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.druid.guice.JsonConfigTesterBase;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TaskLaneConfigTest extends JsonConfigTesterBase<TaskLaneConfig>
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    propertyValues.put(getPropertyKey("labels"), "kill,compact");
    propertyValues.put(getPropertyKey("capacityRatio"), "0.1");
    propertyValues.put(getPropertyKey("policy"), "RESERVE");
    testProperties.putAll(propertyValues);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        ObjectMapper.class,
        new DefaultObjectMapper()
    ));
  }

  @Test
  public void testSerDeserTaskLaneConfig() throws IOException
  {
    TaskLaneConfig taskLaneConfig = new TaskLaneConfig(
        "kill,compact",
        0.1,
        TaskLaneCapacityPolicy.MAX
    );
    String taskLaneConfigString = mapper.writeValueAsString(taskLaneConfig);
    TaskLaneConfig taskLaneConfigExpected = mapper.readerFor(TaskLaneConfig.class).readValue(
        taskLaneConfigString
    );
    Assert.assertEquals(taskLaneConfigExpected, taskLaneConfig);
  }

  @Test
  public void testSerde()
  {
    propertyValues.put(getPropertyKey("labels"), "kill,compact");
    propertyValues.put(getPropertyKey("capacityRatio"), "0.1");
    propertyValues.put(getPropertyKey("policy"), "MAX");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    TaskLaneConfig config = configProvider.get();
    Assert.assertEquals(Sets.newHashSet("kill", "compact"), config.getLabelSet());
    Assert.assertTrue(Double.compare(0.1, config.getCapacityRatio()) == 0);
    Assert.assertEquals(TaskLaneCapacityPolicy.MAX, config.getPolicy());
  }
}