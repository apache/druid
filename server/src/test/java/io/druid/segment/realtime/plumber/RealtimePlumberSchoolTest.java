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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.FilteredServerView;
import io.druid.client.ServerView;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 */
@RunWith(Parameterized.class)
public class RealtimePlumberSchoolTest
{
  private final RejectionPolicyFactory rejectionPolicy;
  private RealtimePlumber plumber;
  private DataSegmentAnnouncer announcer;
  private SegmentPublisher segmentPublisher;
  private DataSegmentPusher dataSegmentPusher;
  private FilteredServerView serverView;
  private ServiceEmitter emitter;
  private RealtimeTuningConfig tuningConfig;
  private DataSchema schema;

  public RealtimePlumberSchoolTest(RejectionPolicyFactory rejectionPolicy)
  {
    this.rejectionPolicy = rejectionPolicy;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[][]{
            {
                new NoopRejectionPolicyFactory()
            },
            {
                new MessageTimeRejectionPolicyFactory()
            }
        }
    );
  }

  @Before
  public void setUp() throws Exception
  {

    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

     schema = new DataSchema(
        "test",
        new InputRowParser()
        {
          @Override
          public InputRow parse(Object input)
          {
            return null;
          }

          @Override
          public ParseSpec getParseSpec()
          {
            return new JSONParseSpec(
                new TimestampSpec("timestamp", "auto"),
                new DimensionsSpec(null, null, null)
            );
          }

          @Override
          public InputRowParser withParseSpec(ParseSpec parseSpec)
          {
            return null;
          }
        },
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.NONE, null)
    );

    announcer = EasyMock.createMock(DataSegmentAnnouncer.class);
    announcer.announceSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().anyTimes();

    segmentPublisher = EasyMock.createNiceMock(SegmentPublisher.class);
    dataSegmentPusher = EasyMock.createNiceMock(DataSegmentPusher.class);
    serverView = EasyMock.createMock(FilteredServerView.class);
    serverView.registerSegmentCallback(
        EasyMock.<Executor>anyObject(),
        EasyMock.<ServerView.SegmentCallback>anyObject(),
        EasyMock.<Predicate<DataSegment>>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();

    emitter = EasyMock.createMock(ServiceEmitter.class);

    EasyMock.replay(announcer, segmentPublisher, dataSegmentPusher, serverView, emitter);

    tuningConfig = new RealtimeTuningConfig(
        1,
        null,
        null,
        null,
        new IntervalStartVersioningPolicy(),
        rejectionPolicy,
        null,
        null,
        null,
        null,
        null
    );

    RealtimePlumberSchool realtimePlumberSchool = new RealtimePlumberSchool(
        emitter,
        new DefaultQueryRunnerFactoryConglomerate(Maps.<Class<? extends Query>, QueryRunnerFactory>newHashMap()),
        dataSegmentPusher,
        announcer,
        segmentPublisher,
        serverView,
        MoreExecutors.sameThreadExecutor()
    );

    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, new FireDepartmentMetrics());
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(announcer, segmentPublisher, dataSegmentPusher, serverView, emitter);
  }

  @Test(timeout = 60000)
  public void testPersist() throws Exception
  {
    final MutableBoolean committed = new MutableBoolean(false);
    plumber.getSinks().put(0L, new Sink(new Interval(0, TimeUnit.HOURS.toMillis(1)),schema, tuningConfig, new DateTime("2014-12-01T12:34:56.789").toString()));
    plumber.startJob();
    final InputRow row = EasyMock.createNiceMock(InputRow.class);
    EasyMock.expect(row.getTimestampFromEpoch()).andReturn(0L);
    EasyMock.expect(row.getDimensions()).andReturn(new ArrayList<String>());
    EasyMock.replay(row);
    plumber.add(row);
    plumber.persist(
        new Runnable()
        {
          @Override
          public void run()
          {
            committed.setValue(true);
          }
        }
    );

    while (!committed.booleanValue()) {
      Thread.sleep(100);
    }
    plumber.getSinks().clear();
    plumber.finishJob();
  }
}
