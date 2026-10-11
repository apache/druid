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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Sets up a Kafka supervisor through the data loader of the web console, then suspends, resumes and terminates it
 * from the Supervisors view.
 */
public class KafkaWebConsoleTest extends WebConsoleTestBase
{
  private static final String TOPIC = "wikipedia";

  private final KafkaResource kafka = new KafkaResource();

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addResource(kafka);
  }

  /**
   * Publishes the edits of the tutorial file to the topic, one per message, in one partition (so in the order of the
   * file), as in the Kafka tutorial.
   */
  @BeforeAll
  public void publishData() throws Exception
  {
    final File dataFile = new File(webConsoleDir(), "../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz");
    final List<byte[]> records = new ArrayList<>();
    // The file is its own resource, to be closed even when reading the gzip header fails
    try (InputStream fileIn = new FileInputStream(dataFile);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(new GZIPInputStream(fileIn), StandardCharsets.UTF_8)
         )) {
      String line;
      while ((line = reader.readLine()) != null) {
        records.add(StringUtils.toUtf8(line));
      }
    }

    kafka.createTopicWithPartitions(TOPIC, 1);
    kafka.publishRecordsToTopic(TOPIC, records);
  }

  @Test
  public void testKafkaIngestion() throws Exception
  {
    runSpec(
        "kafka-ingestion.spec.ts",
        Map.of(
            "DRUID_E2E_TEST_KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServerUrl(),
            "DRUID_E2E_TEST_KAFKA_TOPIC", TOPIC
        )
    );
  }
}
