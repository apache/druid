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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.utils.KinesisAdminClient;
import org.apache.druid.testing.utils.KinesisEventWriter;
import org.apache.druid.testing.utils.StreamAdminClient;
import org.apache.druid.testing.utils.StreamEventWriter;

import javax.annotation.Nullable;
import java.util.function.Function;

public abstract class AbstractKinesisIndexingServiceTest extends AbstractStreamIndexingTest
{
  private static final Logger LOG = new Logger(AbstractKinesisIndexingServiceTest.class);

  @Override
  StreamAdminClient createStreamAdminClient(IntegrationTestingConfig config) throws Exception
  {
    return new KinesisAdminClient(config.getStreamEndpoint());
  }

  @Override
  StreamEventWriter createStreamEventWriter(IntegrationTestingConfig config, @Nullable Boolean transactionEnabled)
      throws Exception
  {
    if (transactionEnabled != null) {
      LOG.warn(
          "Kinesis event writer doesn't support transaction. Ignoring the given parameter transactionEnabled[%s]",
          transactionEnabled
      );
    }
    return new KinesisEventWriter(config.getStreamEndpoint(), false);
  }

  @Override
  Function<String, String> generateStreamIngestionPropsTransform(
      String streamName,
      String fullDatasourceName,
      String parserType,
      String parserOrInputFormat,
      IntegrationTestingConfig config
  )
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
        if (AbstractStreamIndexingTest.INPUT_FORMAT.equals(parserType)) {
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT%%",
              parserOrInputFormat
          );
          spec = StringUtils.replace(
              spec,
              "%%PARSER%%",
              "null"
          );
        } else if (AbstractStreamIndexingTest.INPUT_ROW_PARSER.equals(parserType)) {
          spec = StringUtils.replace(
              spec,
              "%%PARSER%%",
              parserOrInputFormat
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT%%",
              "null"
          );
        }
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
  Function<String, String> generateStreamQueryPropsTransform(String streamName, String fullDatasourceName)
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
}
