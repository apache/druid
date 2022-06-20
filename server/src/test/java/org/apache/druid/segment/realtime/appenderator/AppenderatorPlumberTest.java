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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import org.apache.druid.segment.realtime.plumber.NoopRejectionPolicyFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AppenderatorPlumberTest
{
  private AppenderatorPlumber plumber;
  private StreamAppenderatorTester streamAppenderatorTester;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    this.streamAppenderatorTester =
        new StreamAppenderatorTester.Builder()
            .maxRowsInMemory(10)
            .basePersistDirectory(temporaryFolder.newFolder())
            .build();
    DataSegmentAnnouncer segmentAnnouncer = EasyMock
        .createMock(DataSegmentAnnouncer.class);
    segmentAnnouncer.announceSegment(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    SegmentPublisher segmentPublisher = EasyMock
        .createNiceMock(SegmentPublisher.class);
    SegmentHandoffNotifierFactory handoffNotifierFactory = EasyMock
        .createNiceMock(SegmentHandoffNotifierFactory.class);
    SegmentHandoffNotifier handoffNotifier = EasyMock
        .createNiceMock(SegmentHandoffNotifier.class);
    EasyMock
        .expect(
            handoffNotifierFactory.createSegmentHandoffNotifier(EasyMock
                .anyString())).andReturn(handoffNotifier).anyTimes();
    EasyMock
        .expect(
            handoffNotifier.registerSegmentHandoffCallback(
                EasyMock.anyObject(),
                EasyMock.anyObject(),
                EasyMock.anyObject())).andReturn(true).anyTimes();

    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        null,
        1,
        null,
        null,
        null,
        null,
        temporaryFolder.newFolder(),
        new IntervalStartVersioningPolicy(),
        new NoopRejectionPolicyFactory(),
        null,
        null,
        null,
        null,
        0,
        0,
        false,
        null,
        null,
        null,
        null,
        null
    );

    this.plumber = new AppenderatorPlumber(streamAppenderatorTester.getSchema(),
                                           tuningConfig, streamAppenderatorTester.getMetrics(),
                                           segmentAnnouncer, segmentPublisher, handoffNotifier,
                                           streamAppenderatorTester.getAppenderator());
  }

  @Test
  public void testSimpleIngestion() throws Exception
  {
    Appenderator appenderator = streamAppenderatorTester.getAppenderator();

    // startJob
    Assert.assertEquals(null, plumber.startJob());

    // getDataSource
    Assert.assertEquals(StreamAppenderatorTester.DATASOURCE, appenderator.getDataSource());

    InputRow[] rows = new InputRow[] {
        StreamAppenderatorTest.ir("2000", "foo", 1),
        StreamAppenderatorTest.ir("2000", "bar", 2), StreamAppenderatorTest.ir("2000", "qux", 4)};
    // add
    Assert.assertEquals(1, plumber.add(rows[0], null).getRowCount());

    Assert.assertEquals(2, plumber.add(rows[1], null).getRowCount());

    Assert.assertEquals(3, plumber.add(rows[2], null).getRowCount());

    
    Assert.assertEquals(1, plumber.getSegmentsView().size());
    
    SegmentIdWithShardSpec si = plumber.getSegmentsView().values().toArray(new SegmentIdWithShardSpec[0])[0];
    
    Assert.assertEquals(3, appenderator.getRowCount(si));

    appenderator.clear();    
    Assert.assertTrue(appenderator.getSegments().isEmpty());
    
    plumber.dropSegment(si);
    plumber.finishJob();
  }
}
