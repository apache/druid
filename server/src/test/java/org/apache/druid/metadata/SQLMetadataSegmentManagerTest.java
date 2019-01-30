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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;


public class SQLMetadataSegmentManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private SQLMetadataSegmentManager manager;
  private SQLMetadataSegmentPublisher publisher;
  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  private final DataSegment segment1 = new DataSegment(
      "wikipedia",
      Intervals.of("2012-03-15T00:00:00.000/2012-03-16T00:00:00.000"),
      "2012-03-16T00:36:30.848Z",
      ImmutableMap.of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      NoneShardSpec.instance(),
      0,
      1234L
  );

  private final DataSegment segment2 = new DataSegment(
      "wikipedia",
      Intervals.of("2012-01-05T00:00:00.000/2012-01-06T00:00:00.000"),
      "2012-01-06T22:19:12.565Z",
      ImmutableMap.of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      NoneShardSpec.instance(),
      0,
      1234L
  );

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    manager = new SQLMetadataSegmentManager(
        jsonMapper,
        Suppliers.ofInstance(new MetadataSegmentManagerConfig()),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector
    );

    publisher = new SQLMetadataSegmentPublisher(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector
    );

    connector.createSegmentTable();

    publisher.publishSegment(segment1);
    publisher.publishSegment(segment2);
  }

  @After
  public void teardown()
  {
    if (manager.isStarted()) {
      manager.stop();
    }
  }

  @Test
  public void testPoll()
  {
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.isStarted());
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        manager.getAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(manager.getDataSource("wikipedia").getSegments())
    );
  }

  @Test
  public void testPollWithCurroptedSegment()
  {
    //create a corrupted segment entry in segments table, which tests
    //that overall loading of segments from database continues to work
    //even in one of the entries are corrupted.
    publisher.publishSegment(
        "corrupt-segment-id",
        "corrupt-datasource",
        "corrupt-create-date",
        "corrupt-start-date",
        "corrupt-end-date",
        true,
        "corrupt-version",
        true,
        StringUtils.toUtf8("corrupt-payload")
    );

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.isStarted());

    Assert.assertEquals(
        "wikipedia", Iterables.getOnlyElement(manager.getDataSources()).getName()
    );
  }

  @Test
  public void testGetUnusedSegmentsForInterval()
  {
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.isStarted());
    Assert.assertTrue(manager.removeDataSource("wikipedia"));

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        manager.getUnusedSegmentIntervals("wikipedia", Intervals.of("1970/3000"), 1)
    );

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval(), segment1.getInterval()),
        manager.getUnusedSegmentIntervals("wikipedia", Intervals.of("1970/3000"), 5)
    );
  }

  @Test
  public void testRemoveDataSource() throws IOException
  {
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.isStarted());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment = new DataSegment(
        newDataSource,
        Intervals.of("2017-10-15T00:00:00.000/2017-10-16T00:00:00.000"),
        "2017-10-15T20:19:12.565Z",
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip"
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        0,
        1234L
    );

    publisher.publishSegment(newSegment);

    Assert.assertNull(manager.getDataSource(newDataSource));
    Assert.assertTrue(manager.removeDataSource(newDataSource));
  }

  @Test
  public void testRemoveDataSegment() throws IOException
  {
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.isStarted());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment = new DataSegment(
        newDataSource,
        Intervals.of("2017-10-15T00:00:00.000/2017-10-16T00:00:00.000"),
        "2017-10-15T20:19:12.565Z",
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip"
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        0,
        1234L
    );

    publisher.publishSegment(newSegment);

    Assert.assertNull(manager.getDataSource(newDataSource));
    Assert.assertTrue(manager.removeSegment(newSegment.getId()));
  }

  @Test
  public void testStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    manager.start();
    manager.stop();
    manager.start();
    manager.stop();
  }
}
