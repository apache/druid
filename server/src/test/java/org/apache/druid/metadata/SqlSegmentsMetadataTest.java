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
import org.apache.druid.java.util.common.DateTimes;
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


public class SqlSegmentsMetadataTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private SqlSegmentsMetadata sqlSegmentsMetadata;
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
    sqlSegmentsMetadata = new SqlSegmentsMetadata(
        jsonMapper,
        Suppliers.ofInstance(new SegmentsMetadataConfig()),
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
    if (sqlSegmentsMetadata.isStarted()) {
      sqlSegmentsMetadata.stop();
    }
  }

  @Test
  public void testPoll()
  {
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isStarted());
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        sqlSegmentsMetadata.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.prepareImmutableDataSourceWithUsedSegments("wikipedia").getSegments())
    );
  }

  @Test
  public void testPollWithCorruptedSegment()
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
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isStarted());

    Assert.assertEquals(
        "wikipedia", Iterables.getOnlyElement(sqlSegmentsMetadata.prepareImmutableDataSourcesWithAllUsedSegments()).getName()
    );
  }

  @Test
  public void testGetUnusedSegmentsForInterval()
  {
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isStarted());
    int numChangedSegments = sqlSegmentsMetadata.markAsUnusedAllSegmentsInDataSource("wikipedia");
    Assert.assertEquals(2, numChangedSegments);

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        sqlSegmentsMetadata.getUnusedSegmentIntervals("wikipedia", DateTimes.of("3000"), 1)
    );

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval(), segment1.getInterval()),
        sqlSegmentsMetadata.getUnusedSegmentIntervals("wikipedia", DateTimes.of("3000"), 5)
    );
  }

  @Test
  public void testMarkAsUnusedAllSegmentsInDataSource() throws IOException
  {
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isStarted());

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

    Assert.assertNull(sqlSegmentsMetadata.prepareImmutableDataSourceWithUsedSegments(newDataSource));
    int numChangedSegments = sqlSegmentsMetadata.markAsUnusedAllSegmentsInDataSource(newDataSource);
    Assert.assertEquals(1, numChangedSegments);
  }

  @Test
  public void testMarkSegmentAsUnused() throws IOException
  {
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isStarted());

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

    Assert.assertNull(sqlSegmentsMetadata.prepareImmutableDataSourceWithUsedSegments(newDataSource));
    Assert.assertTrue(sqlSegmentsMetadata.markSegmentAsUnused(newSegment.getId()));
  }

  @Test
  public void testStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.stop();
    sqlSegmentsMetadata.start();
    sqlSegmentsMetadata.stop();
  }
}
