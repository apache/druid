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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.testing.embedded.indexing.KafkaTestBase;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

/**
 * Contains miscellaneous tests for Kafka supervisors.
 */
public class KafkaMultiSupervisorTest extends KafkaTestBase
{
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_ingestIntoSingleDatasource_fromDifferentTopics(boolean useTransactions)
  {
    final List<DimensionSchema> dimensionsSpec = DimensionsSpec.getDefaultSchemas(
        List.of("kafka.topic", WikipediaStreamEventStreamGenerator.COL_UNIQUE_NAMESPACE)
    );

    // Set up first topic and supervisor
    final String topic1 = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic1, 1);

    final KafkaSupervisorSpec supervisor1 = createSupervisor()
        .withIoConfig(io -> io.withKafkaInputFormat(new JsonInputFormat(null, null, null, null, null)))
        .withDataSchema(d -> d.withDimensions(dimensionsSpec))
        .withId(topic1)
        .build(dataSource, topic1);
    cluster.callApi().postSupervisor(supervisor1);

    // Set up another topic and supervisor
    final String topic2 = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic2, 1);

    final KafkaSupervisorSpec supervisor2 = createSupervisor()
        .withIoConfig(io -> io.withKafkaInputFormat(new JsonInputFormat(null, null, null, null, null)))
        .withDataSchema(d -> d.withDimensions(dimensionsSpec))
        .withId(topic2)
        .build(dataSource, topic2);
    cluster.callApi().postSupervisor(supervisor2);

    // Produce records to both topics
    publishRecords(topic1, useTransactions);
    publishRecords(topic2, useTransactions);
    waitUntilPublishedRecordsAreIngested();

    // Tear down both topics and supervisors
    kafkaServer.deleteTopic(topic1);
    cluster.callApi().postSupervisor(supervisor1.createSuspendedSpec());
    kafkaServer.deleteTopic(topic2);
    cluster.callApi().postSupervisor(supervisor2.createSuspendedSpec());

    // Verify total row count and row count for each topic
    verifyRowCount();
    Assertions.assertEquals(
        "60",
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE \"kafka.topic\" = '%s'", dataSource, topic1)
    );
    Assertions.assertEquals(
        "60",
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE \"kafka.topic\" = '%s'", dataSource, topic2)
    );
  }
}
