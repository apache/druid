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
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Test;

public class UnionQueryTest extends EmbeddedClusterTestBase
{
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
  public void testUnionQuery()
  {
    final int numDatasources = 3;
    for (int i = 0; i < numDatasources; i++) {

      // just write some data

      // wait for data to be ingested

      // wait for all segments to be loaded
    }

    // run and verify the queries
  }
}
