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

package io.druid.indexing.common;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TaskLockSerdeTest
{

  private ObjectMapper jsonMapper;
  @Before
  public void setUp(){
    jsonMapper = new DefaultObjectMapper();
  }

  @Test
  public void testUpgradedTaskLockSerde() throws IOException{
    String taskLockString = "{\"groupId\":\"group1\",\"dataSource\":\"DS\","
                            + "\"interval\":\"2015-07-01T00:00:00.000Z/2015-07-03T00:00:00.000Z\","
                            + "\"version\":\"2015-08-31T16:38:41.661Z\",\"priority\":0, \"upgraded\": true}";
    TaskLock taskLock = new TaskLock("group1", "DS", new Interval("2015-07-01T00:00:00.000Z/2015-07-03T00:00:00.000Z"), "2015-08-31T16:38:41.661Z", 0, true);
    TaskLock taskLockSerde = jsonMapper.readValue(taskLockString, TaskLock.class);
    Assert.assertEquals(taskLock, taskLockSerde);
  }

  @Test
  public void testBasicTaskLockSerde() throws IOException{
    String taskLockString = "{\"groupId\":\"group1\",\"dataSource\":\"DS\","
                            + "\"interval\":\"2015-07-01T00:00:00.000Z/2015-07-03T00:00:00.000Z\","
                            + "\"version\":\"2015-08-31T16:38:41.661Z\", \"priority\":34}";
    TaskLock taskLock = new TaskLock("group1", "DS", new Interval("2015-07-01T00:00:00.000Z/2015-07-03T00:00:00.000Z"), "2015-08-31T16:38:41.661Z", 34, false);
    TaskLock taskLockSerde = jsonMapper.readValue(taskLockString, TaskLock.class);
    Assert.assertEquals(taskLock, taskLockSerde);
  }
}
