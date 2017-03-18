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

package io.druid.timeline;

import io.druid.timeline.DataSegmentUtils.SegmentIdentifierParts;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DataSegmentUtilsTest
{
  @Test
  public void testBasic()
  {
    String datasource = "datasource";
    SegmentIdentifierParts desc = new SegmentIdentifierParts(datasource, new Interval("2015-01-02/2015-01-03"), "ver", "0_0");
    Assert.assertEquals("datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_0", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_0", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = new SegmentIdentifierParts(datasource, new Interval("2015-01-02/2015-01-03"), "ver", null);
    Assert.assertEquals("datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));
  }

  @Test
  public void testDataSourceWithUnderscore1()
  {
    String datasource = "datasource_1";
    SegmentIdentifierParts desc = new SegmentIdentifierParts(datasource, new Interval("2015-01-02/2015-01-03"), "ver", "0_0");
    Assert.assertEquals("datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_0", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_1_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_0", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = new SegmentIdentifierParts(datasource, new Interval("2015-01-02/2015-01-03"), "ver", null);
    Assert.assertEquals("datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_1_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(datasource, desc.toString()));
  }

  @Test
  public void testDataSourceWithUnderscore2()
  {
    String dataSource = "datasource_2015-01-01T00:00:00.000Z";
    SegmentIdentifierParts desc = new SegmentIdentifierParts(dataSource, new Interval("2015-01-02/2015-01-03"), "ver", "0_0");
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_0",
        desc.toString()
    );
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(dataSource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_0",
        desc.toString()
    );
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(dataSource, desc.toString()));

    desc = new SegmentIdentifierParts(dataSource, new Interval("2015-01-02/2015-01-03"), "ver", null);
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver",
        desc.toString()
    );
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(dataSource, desc.toString()));

    desc = desc.withInterval(new Interval("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver",
        desc.toString()
    );
    Assert.assertEquals(desc, DataSegmentUtils.valueOf(dataSource, desc.toString()));
  }

  @Test
  public void testInvalidFormat0()
  {
    Assert.assertNull(DataSegmentUtils.valueOf("ds", "datasource_2015-01-02T00:00:00.000Z_2014-10-20T00:00:00.000Z_version"));
  }

  @Test
  public void testInvalidFormat1()
  {
    Assert.assertNull(DataSegmentUtils.valueOf("datasource", "datasource_invalid_interval_version"));
  }

  @Test
  public void testInvalidFormat2()
  {
    Assert.assertNull(DataSegmentUtils.valueOf("datasource", "datasource_2015-01-02T00:00:00.000Z_version"));
  }
}
