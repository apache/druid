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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.tools.ITRetryUtil;
import org.apache.druid.testing.tools.KafkaEventWriter;
import org.apache.druid.testing.tools.StreamEventWriter;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class UnionQueryTest extends EmbeddedClusterTestBase
{
  private static final Logger LOG = new Logger(UnionQueryTest.class);
  private static final String UNION_SUPERVISOR_TEMPLATE = "/query/union_kafka_supervisor_template.json";
  private static final String UNION_DATA_FILE = "/query/union_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/query/union_queries.json";

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final KafkaResource kafkaResource = new KafkaResource();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addResource(kafkaResource)
        .addServer(overlord);
  }

  @Test
  public void testUnionQuery() throws Exception
  {
    final String baseName = EmbeddedClusterApis.createTestDatasourceName();
    List<String> supervisors = new ArrayList<>();

    final int numDatasources = 3;
    for (int i = 0; i < numDatasources; i++) {
      String datasource = baseName + "-" + i;
      kafkaResource.createTopicWithPartitions(datasource, 1);
      ITRetryUtil.retryUntil(
          () -> kafkaResource.isStreamActive(datasource),
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
      kafkaResource.deleteStream(baseName + "-" + i);
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

    // unload everything
  }
}
