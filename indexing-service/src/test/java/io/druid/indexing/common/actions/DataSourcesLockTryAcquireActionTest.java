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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.indexing.overlord.DataSourceAndInterval;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DataSourcesLockTryAcquireActionTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    DataSourcesLockTryAcquireAction expected = new DataSourcesLockTryAcquireAction(
        ImmutableList.of(
            new DataSourceAndInterval("A", new Interval("2015-01-01/2015-01-02")),
            new DataSourceAndInterval("B", new Interval("2015-01-01/2015-01-03"))
        )
    );

    DataSourcesLockTryAcquireAction actual = (DataSourcesLockTryAcquireAction) mapper.readValue(
        mapper.writeValueAsString(expected),
        TaskAction.class
    );

    Assert.assertEquals(expected, actual);
  }
}

