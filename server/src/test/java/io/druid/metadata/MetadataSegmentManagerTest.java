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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class MetadataSegmentManagerTest
{
  private SQLMetadataSegmentManager manager;
  private TestDerbyConnector connector;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final DataSegment segment1 = new DataSegment(
      "wikipedia",
      new Interval("2012-03-15T00:00:00.000/2012-03-16T00:00:00.000"),
      "2012-03-16T00:36:30.848Z",
      ImmutableMap.<String, Object>of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      new NoneShardSpec(),
      0,
      1234L
  );

  private final DataSegment segment2 = new DataSegment(
      "wikipedia",
      new Interval("2012-01-05T00:00:00.000/2012-01-06T00:00:00.000"),
      "2012-01-06T22:19:12.565Z",
      ImmutableMap.<String, Object>of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      new NoneShardSpec(),
      0,
      1234L
  );

  @Before
  public void setUp() throws Exception
  {
    final Supplier<MetadataStorageTablesConfig> dbTables = Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase("test"));

    connector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        dbTables
    );

    manager = new SQLMetadataSegmentManager(
        jsonMapper,
        Suppliers.ofInstance(new MetadataSegmentManagerConfig()),
        dbTables,
        connector
    );

    SQLMetadataSegmentPublisher publisher = new SQLMetadataSegmentPublisher(
        jsonMapper,
        dbTables.get(),
        connector
    );

    connector.createSegmentTable();

    publisher.publishSegment(segment1);
    publisher.publishSegment(segment2);
  }

  @After
  public void tearDown() throws Exception
  {
    connector.tearDown();
  }

  @Test
  public void testPoll()
  {
    manager.start();
    manager.poll();
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        manager.getAllDatasourceNames()
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        manager.getInventoryValue("wikipedia").getSegments()
    );
    manager.stop();
  }
}
