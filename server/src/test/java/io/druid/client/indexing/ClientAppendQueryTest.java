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

package io.druid.client.indexing;

import com.google.common.collect.Lists;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ClientAppendQueryTest
{
  private ClientAppendQuery clientAppendQuery;
  private static final String DATA_SOURCE = "data_source";
  private List<DataSegment> segments = Lists.<DataSegment>newArrayList(
      new DataSegment(DATA_SOURCE, new Interval(new DateTime(), new DateTime().plus(1)), new DateTime().toString(), null,
          null, null, null, 0, 0));
  @Before
  public void setUp()
  {
    clientAppendQuery = new ClientAppendQuery(DATA_SOURCE, segments);
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals("append",clientAppendQuery.getType());
  }

  @Test
  public void testGetDataSource()
  {
    Assert.assertEquals(DATA_SOURCE, clientAppendQuery.getDataSource());
  }

  @Test
  public void testGetSegments()
  {
    Assert.assertEquals(segments, clientAppendQuery.getSegments());
  }

  @Test
  public void testToString()
  {
    Assert.assertTrue(clientAppendQuery.toString().contains(DATA_SOURCE));
    Assert.assertTrue(clientAppendQuery.toString().contains(segments.toString()));
  }
}
