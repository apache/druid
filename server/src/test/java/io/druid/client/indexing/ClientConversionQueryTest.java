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

import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class ClientConversionQueryTest
{
  private ClientConversionQuery clientConversionQuery;
  private static final String DATA_SOURCE = "data_source";
  private static final Interval INTERVAL = new Interval(new DateTime(), new DateTime().plus(1));
  private static final DataSegment DATA_SEGMENT = new DataSegment(DATA_SOURCE, INTERVAL, new DateTime().toString(), null,
      null, null, null, 0, 0);

  @Test
  public void testGetType()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals("version_converter", clientConversionQuery.getType());
  }

  @Test
  public void testGetDataSource()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(DATA_SOURCE, clientConversionQuery.getDataSource());

  }

  @Test
  public void testGetInterval()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(INTERVAL, clientConversionQuery.getInterval());
  }

  @Test
  public void testGetSegment()
  {
    clientConversionQuery = new ClientConversionQuery(DATA_SEGMENT);
    Assert.assertEquals(DATA_SEGMENT, clientConversionQuery.getSegment());
    clientConversionQuery = new ClientConversionQuery(DATA_SOURCE,INTERVAL);
    Assert.assertNull(clientConversionQuery.getSegment());
  }
}
