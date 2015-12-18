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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ClientMergeQueryTest
{
  private static final String DATA_SOURCE = "data_source";
  private static final Interval INTERVAL = new Interval(new DateTime(), new DateTime().plus(1));
  private static final DataSegment DATA_SEGMENT = new DataSegment(DATA_SOURCE, INTERVAL, new DateTime().toString(), null,
      null, null, null, 0, 0);
  private static final List<DataSegment> SEGMENT_LIST = Lists.newArrayList(DATA_SEGMENT);
  private static final List<AggregatorFactory> AGGREGATOR_LIST = Lists.newArrayList();
  private static final ClientMergeQuery CLIENT_MERGE_QUERY = new ClientMergeQuery(DATA_SOURCE,SEGMENT_LIST,AGGREGATOR_LIST);;

  @Test
  public void testGetType()
  {
    Assert.assertEquals("merge", CLIENT_MERGE_QUERY.getType());
  }

  @Test
  public void testGetDataSource()
  {
    Assert.assertEquals(DATA_SOURCE, CLIENT_MERGE_QUERY.getDataSource());
  }

  @Test
  public void testGetSegments()
  {
    Assert.assertEquals(SEGMENT_LIST, CLIENT_MERGE_QUERY.getSegments());
  }

  @Test
  public void testGetAggregators()
  {
    Assert.assertEquals(AGGREGATOR_LIST, CLIENT_MERGE_QUERY.getAggregators());
  }

  @Test
  public void testToString()
  {
    Assert.assertTrue(CLIENT_MERGE_QUERY.toString().contains(DATA_SOURCE));
    Assert.assertTrue(CLIENT_MERGE_QUERY.toString().contains(SEGMENT_LIST.toString()));
    Assert.assertTrue(CLIENT_MERGE_QUERY.toString().contains(AGGREGATOR_LIST.toString()));
  }
}
