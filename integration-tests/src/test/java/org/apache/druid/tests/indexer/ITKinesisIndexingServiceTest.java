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

package org.apache.druid.tests.indexer;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.KinesisAdminClient;
import org.apache.druid.testing.utils.KinesisEventWriter;
import org.apache.druid.testing.utils.StreamAdminClient;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.function.Function;

@Test(groups = TestNGGroup.KINESIS_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKinesisIndexingServiceTest extends AbstractStreamIndexingTest
{

  @Override
  StreamAdminClient getStreamAdminClient() throws Exception
  {
    return new KinesisAdminClient(config.getStreamEndpoint());
  }

  @Override
  StreamEventWriter getStreamEventWriter() throws Exception
  {
    return new KinesisEventWriter(config.getStreamEndpoint(), false);
  }

  @Override
  Function<String, String> getStreamIngestionPropsTransform()
  {
    return spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            fullDatasourceName
        );
        spec = StringUtils.replace(
            spec,
            "%%STREAM_TYPE%%",
            "kinesis"
        );
        spec = StringUtils.replace(
            spec,
            "%%TOPIC_KEY%%",
            "stream"
        );
        spec = StringUtils.replace(
            spec,
            "%%TOPIC_VALUE%%",
            streamName
        );
        spec = StringUtils.replace(
            spec,
            "%%USE_EARLIEST_KEY%%",
            "useEarliestSequenceNumber"
        );
        spec = StringUtils.replace(
            spec,
            "%%STREAM_PROPERTIES_KEY%%",
            "endpoint"
        );
        return StringUtils.replace(
            spec,
            "%%STREAM_PROPERTIES_VALUE%%",
            jsonMapper.writeValueAsString(config.getStreamEndpoint())
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  Function<String, String> getStreamQueryPropsTransform()
  {
    return spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            fullDatasourceName
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_MAXTIME%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME.plusSeconds(TOTAL_NUMBER_OF_SECOND - 1))
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_MINTIME%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_QUERY_START%%",
            INTERVAL_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_QUERY_END%%",
            INTERVAL_FMT.print(FIRST_EVENT_TIME.plusSeconds(TOTAL_NUMBER_OF_SECOND - 1).plusMinutes(2))
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_RESPONSE_TIMESTAMP%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_ADDED%%",
            Long.toString(getSumOfEventSequence(EVENTS_PER_SECOND) * TOTAL_NUMBER_OF_SECOND)
        );
        return StringUtils.replace(
            spec,
            "%%TIMESERIES_NUMEVENTS%%",
            Integer.toString(EVENTS_PER_SECOND * TOTAL_NUMBER_OF_SECOND)
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Test
  public void testKinesisIndexDataWithLegacyParserStableState() throws Exception
  {
    doTestIndexDataWithLegacyParserStableState();
  }

  @Test
  public void testKinesisIndexDataWithInputFormatStableState() throws Exception
  {
    doTestIndexDataWithInputFormatStableState();
  }

  @Test
  public void testKinesisIndexDataWithLosingCoordinator() throws Exception
  {
    doTestIndexDataWithLosingCoordinator();
  }

  @Test
  public void testKinesisIndexDataWithLosingOverlord() throws Exception
  {
    doTestIndexDataWithLosingOverlord();
  }

  @Test
  public void testKinesisIndexDataWithLosingHistorical() throws Exception
  {
    doTestIndexDataWithLosingHistorical();
  }

  @Test
  public void testKinesisIndexDataWithStartStopSupervisor() throws Exception
  {
    doTestIndexDataWithStartStopSupervisor();
  }

  @Test
  public void testKinesisIndexDataWithKinesisReshardSplit() throws Exception
  {
    doTestIndexDataWithKinesisReshardSplit();
  }

  @Test
  public void testKinesisIndexDataWithKinesisReshardMerge() throws Exception
  {
    doTestIndexDataWithKinesisReshardMerge();
  }
}
