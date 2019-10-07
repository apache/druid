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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.utils.RetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

abstract class AbstractKafkaIndexerTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(AbstractKafkaIndexerTest.class);
  private static final String INDEXER_FILE = "/indexer/kafka_supervisor_spec.json";
  private static final String QUERIES_FILE = "/indexer/kafka_index_queries.json";
  private static final String TOPIC_NAME = "kafka_indexing_service_topic";

  private static final int NUM_EVENTS_TO_SEND = 60;
  private static final long WAIT_TIME_MILLIS = 2 * 60 * 1000L;
  private static final String TEST_PROPERTY_PREFIX = "kafka.test.property.";

  // We'll fill in the current time and numbers for added, deleted and changed
  // before sending the event.
  private static final String EVENT_TEMPLATE =
      "{\"timestamp\": \"%s\"," +
      "\"page\": \"Gypsy Danger\"," +
      "\"language\" : \"en\"," +
      "\"user\" : \"nuclear\"," +
      "\"unpatrolled\" : \"true\"," +
      "\"newPage\" : \"true\"," +
      "\"robot\": \"false\"," +
      "\"anonymous\": \"false\"," +
      "\"namespace\":\"article\"," +
      "\"continent\":\"North America\"," +
      "\"country\":\"United States\"," +
      "\"region\":\"Bay Area\"," +
      "\"city\":\"San Francisco\"," +
      "\"added\":%d," +
      "\"deleted\":%d," +
      "\"delta\":%d}";

  private ZkUtils zkUtils;
  private boolean segmentsExist;   // to tell if we should remove segments during teardown

  // format for the querying interval
  private static final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  private static final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");

  @Inject
  private TestQueryHelper queryHelper;
  @Inject
  private IntegrationTestingConfig config;

  private String fullDatasourceName;

  void doKafkaIndexTest(String dataSourceName, boolean txnEnabled)
  {
    fullDatasourceName = dataSourceName + config.getExtraDatasourceNameSuffix();
    // create topic
    try {
      int sessionTimeoutMs = 10000;
      int connectionTimeoutMs = 10000;
      String zkHosts = config.getZookeeperHosts();
      ZkClient zkClient = new ZkClient(zkHosts, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
      zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts, sessionTimeoutMs), false);
      if (config.manageKafkaTopic()) {
        int numPartitions = 4;
        int replicationFactor = 1;
        Properties topicConfig = new Properties();
        AdminUtils.createTopic(
            zkUtils,
            TOPIC_NAME,
            numPartitions,
            replicationFactor,
            topicConfig,
            RackAwareMode.Disabled$.MODULE$
        );
      }
    }
    catch (Exception e) {
      throw new ISE(e, "could not create kafka topic");
    }

    String spec;
    try {
      LOG.info("supervisorSpec name: [%s]", INDEXER_FILE);
      final Map<String, Object> consumerConfigs = KafkaConsumerConfigs.getConsumerProperties();
      final Properties consumerProperties = new Properties();
      consumerProperties.putAll(consumerConfigs);
      consumerProperties.setProperty("bootstrap.servers", config.getKafkaInternalHost());

      spec = getResourceAsString(INDEXER_FILE);
      spec = StringUtils.replace(spec, "%%DATASOURCE%%", fullDatasourceName);
      spec = StringUtils.replace(spec, "%%TOPIC%%", TOPIC_NAME);
      spec = StringUtils.replace(spec, "%%CONSUMER_PROPERTIES%%", jsonMapper.writeValueAsString(consumerProperties));
      LOG.info("supervisorSpec: [%s]\n", spec);
    }
    catch (Exception e) {
      LOG.error("could not read file [%s]", INDEXER_FILE);
      throw new ISE(e, "could not read file [%s]", INDEXER_FILE);
    }

    // start supervisor
    String supervisorId = indexer.submitSupervisor(spec);
    LOG.info("Submitted supervisor");

    // set up kafka producer
    Properties properties = new Properties();
    addFilteredProperties(config, properties);
    properties.setProperty("bootstrap.servers", config.getKafkaHost());
    LOG.info("Kafka bootstrap.servers: [%s]", config.getKafkaHost());
    properties.setProperty("acks", "all");
    properties.setProperty("retries", "3");
    properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
    properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
    if (txnEnabled) {
      properties.setProperty("enable.idempotence", "true");
      properties.setProperty("transactional.id", RandomIdUtils.getRandomId());
    }

    KafkaProducer<String, String> producer = new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new StringSerializer()
    );

    DateTimeZone zone = DateTimes.inferTzFromString("UTC");
    // format for putting into events
    DateTimeFormatter event_fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    DateTime dt = new DateTime(zone); // timestamp to put on events
    DateTime dtFirst = dt;            // timestamp of 1st event
    DateTime dtLast = dt;             // timestamp of last event

    // these are used to compute the expected aggregations
    int added = 0;
    int num_events = 0;

    // send data to kafka
    if (txnEnabled) {
      producer.initTransactions();
      producer.beginTransaction();
    }
    while (num_events < NUM_EVENTS_TO_SEND) {
      num_events++;
      added += num_events;
      // construct the event to send
      String event = StringUtils.format(EVENT_TEMPLATE, event_fmt.print(dt), num_events, 0, num_events);
      LOG.info("sending event: [%s]", event);
      try {

        producer.send(new ProducerRecord<>(TOPIC_NAME, event)).get();

      }
      catch (Exception ioe) {
        throw Throwables.propagate(ioe);
      }

      dtLast = dt;
      dt = new DateTime(zone);
    }
    if (txnEnabled) {
      producer.commitTransaction();
    }
    producer.close();

    LOG.info("Waiting for [%s] millis for Kafka indexing tasks to consume events", WAIT_TIME_MILLIS);
    try {
      Thread.sleep(WAIT_TIME_MILLIS);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    InputStream is = AbstractKafkaIndexerTest.class.getResourceAsStream(QUERIES_FILE);
    if (null == is) {
      throw new ISE("could not open query file: %s", QUERIES_FILE);
    }

    // put the timestamps into the query structure
    String query_response_template;
    try {
      query_response_template = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", QUERIES_FILE);
    }

    String queryStr = query_response_template;
    queryStr = StringUtils.replace(queryStr, "%%DATASOURCE%%", fullDatasourceName);
    queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst));
    queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_MAXTIME%%", TIMESTAMP_FMT.print(dtLast));
    queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_MINTIME%%", TIMESTAMP_FMT.print(dtFirst));
    queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_QUERY_START%%", INTERVAL_FMT.print(dtFirst));
    queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_QUERY_END%%", INTERVAL_FMT.print(dtLast.plusMinutes(2)));
    queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst));
    queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_ADDED%%", Integer.toString(added));
    queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_NUMEVENTS%%", Integer.toString(num_events));

    // this query will probably be answered from the indexing tasks but possibly from 2 historical segments / 2 indexing
    try {
      this.queryHelper.testQueriesFromString(queryStr, 2);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Shutting down Kafka Supervisor");
    indexer.shutdownSupervisor(supervisorId);

    // wait for all kafka indexing tasks to finish
    LOG.info("Waiting for all kafka indexing tasks to finish");
    RetryUtil.retryUntilTrue(
        () -> (indexer.getPendingTasks().size()
               + indexer.getRunningTasks().size()
               + indexer.getWaitingTasks().size()) == 0,
        "Waiting for Tasks Completion"
    );

    // wait for segments to be handed off
    try {
      RetryUtil.retryUntil(
          () -> coordinator.areSegmentsLoaded(fullDatasourceName),
          true,
          10000,
          30,
          "Real-time generated segments loaded"
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    LOG.info("segments are present");
    segmentsExist = true;

    // this query will be answered by at least 1 historical segment, most likely 2, and possibly up to all 4
    try {
      this.queryHelper.testQueriesFromString(queryStr, 2);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  
  private void addFilteredProperties(IntegrationTestingConfig config, Properties properties)
  {
    for (Map.Entry<String, String> entry : config.getProperties().entrySet()) {
      if (entry.getKey().startsWith(TEST_PROPERTY_PREFIX)) {
        properties.setProperty(entry.getKey().substring(TEST_PROPERTY_PREFIX.length()), entry.getValue());
      }
    }
  }

  void doTearDown()
  {
    if (config.manageKafkaTopic()) {
      // delete kafka topic
      AdminUtils.deleteTopic(zkUtils, TOPIC_NAME);
    }

    // remove segments
    if (segmentsExist && fullDatasourceName != null) {
      unloadAndKillData(fullDatasourceName);
    }
  }
}
