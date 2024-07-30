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

package org.apache.druid.testsEx.query;

import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.KafkaAdminClient;
import org.apache.druid.testing.utils.KafkaEventWriter;
import org.apache.druid.testing.utils.KafkaUtil;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testsEx.indexer.AbstractIndexerTest;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

public class UnionQueryTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(UnionQueryTest.class);
  private static final String UNION_SUPERVISOR_TEMPLATE = "/query/union_kafka_supervisor_template.json";
  private static final String UNION_DATA_FILE = "/query/union_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/query/union_queries.json";
  private static final String UNION_DATASOURCE = "wikipedia_index_test";
  private String fullDatasourceName;

  @Test
  public void testUnionQuery() throws Exception
  {
    fullDatasourceName = UNION_DATASOURCE + config.getExtraDatasourceNameSuffix();
    final String baseName = fullDatasourceName + UUID.randomUUID();
    KafkaAdminClient streamAdminClient = new KafkaAdminClient(config);
    List<String> supervisors = new ArrayList<>();

    final int numDatasources = 3;
    for (int i = 0; i < numDatasources; i++) {
      String datasource = baseName + "-" + i;
      streamAdminClient.createStream(datasource, 1, Collections.emptyMap());
      ITRetryUtil.retryUntil(
          () -> streamAdminClient.isStreamActive(datasource),
          true,
          10000,
          30,
          "Wait for stream active"
      );
      String supervisorSpec = generateStreamIngestionPropsTransform(
          datasource,
          datasource,
          config
      ).apply(getResourceAsString(UNION_SUPERVISOR_TEMPLATE));
      LOG.info("supervisorSpec: [%s]\n", supervisorSpec);
      // Start supervisor
      String specResponse = indexer.submitSupervisor(supervisorSpec);
      LOG.info("Submitted supervisor [%s]", specResponse);
      supervisors.add(specResponse);

      int ctr = 0;
      try (
          StreamEventWriter streamEventWriter = new KafkaEventWriter(config, false);
          BufferedReader reader = new BufferedReader(
              new InputStreamReader(getResourceAsStream(UNION_DATA_FILE), StandardCharsets.UTF_8)
          )
      ) {
        String line;
        while ((line = reader.readLine()) != null) {
          streamEventWriter.write(datasource, StringUtils.toUtf8(line));
          ctr++;
        }
      }
      final int numWritten = ctr;

      LOG.info("Waiting for stream indexing tasks to consume events");

      ITRetryUtil.retryUntilTrue(
          () ->
              numWritten == this.queryHelper.countRows(
                  datasource,
                  Intervals.ETERNITY,
                  name -> new LongSumAggregatorFactory(name, "count")
              ),
          StringUtils.format(
              "dataSource[%s] consumed [%,d] events, expected [%,d]",
              datasource,
              this.queryHelper.countRows(
                  datasource,
                  Intervals.ETERNITY,
                  name -> new LongSumAggregatorFactory(name, "count")
              ),
              numWritten
          )
      );
    }

    String queryResponseTemplate = StringUtils.replace(
        getResourceAsString(UNION_QUERIES_RESOURCE),
        "%%DATASOURCE%%",
        baseName
    );

    queryHelper.testQueriesFromString(queryResponseTemplate);


    for (int i = 0; i < numDatasources; i++) {
      indexer.terminateSupervisor(supervisors.get(i));
      streamAdminClient.deleteStream(baseName + "-" + i);
    }

    for (int i = 0; i < numDatasources; i++) {
      final int datasourceNumber = i;
      ITRetryUtil.retryUntil(
          () -> coordinator.areSegmentsLoaded(baseName + "-" + datasourceNumber),
          true,
          10000,
          10,
          "Kafka segments loaded"
      );
    }

    queryHelper.testQueriesFromString(queryResponseTemplate);

    for (int i = 0; i < numDatasources; i++) {
      final String datasource = baseName + "-" + i;
      List<String> intervals = coordinator.getSegmentIntervals(datasource);

      Collections.sort(intervals);
      String first = intervals.get(0).split("/")[0];
      String last = intervals.get(intervals.size() - 1).split("/")[1];
      Interval interval = Intervals.of(first + "/" + last);
      coordinator.unloadSegmentsForDataSource(baseName + "-" + i);
      ITRetryUtil.retryUntilFalse(
          () -> coordinator.areSegmentsLoaded(datasource),
          "Segment Unloading"
      );
      coordinator.deleteSegmentsDataSource(baseName + "-" + i, interval);
    }
  }


  /**
   * sad version of
   * {@link org.apache.druid.tests.indexer.AbstractKafkaIndexingServiceTest#generateStreamIngestionPropsTransform}
   */
  private Function<String, String> generateStreamIngestionPropsTransform(
      String streamName,
      String fullDatasourceName,
      IntegrationTestingConfig config
  )
  {
    final Map<String, Object> consumerConfigs = KafkaConsumerConfigs.getConsumerProperties();
    final Properties consumerProperties = new Properties();
    consumerProperties.putAll(consumerConfigs);
    consumerProperties.setProperty("bootstrap.servers", config.getKafkaInternalHost());
    KafkaUtil.addPropertiesFromTestConfig(config, consumerProperties);
    return spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            fullDatasourceName
        );
        spec = StringUtils.replace(
            spec,
            "%%TOPIC_VALUE%%",
            streamName
        );
        return StringUtils.replace(
            spec,
            "%%STREAM_PROPERTIES_VALUE%%",
            jsonMapper.writeValueAsString(consumerProperties)
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
