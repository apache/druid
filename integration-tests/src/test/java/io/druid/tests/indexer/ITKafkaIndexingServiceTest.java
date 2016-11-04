/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.tests.indexer;

import com.google.common.base.Throwables;
import com.google.inject.Inject;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.TestQueryHelper;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

/*
 * This is a test for the Kafka indexing service.
 */
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKafkaIndexingServiceTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITKafkaIndexingServiceTest.class);
  private static final int DELAY_BETWEEN_EVENTS_SECS = 5;
  private static final String INDEXER_FILE = "/indexer/kafka_supervisor_spec.json";
  private static final String QUERIES_FILE = "/indexer/kafka_index_queries.json";
  private static final String DATASOURCE = "kafka_indexing_service_test";
  private static final String TOPIC_NAME = "kafka_indexing_service_topic";
  private static final int MINUTES_TO_SEND = 4;

  // We'll fill in the current time and numbers for added, deleted and changed
  // before sending the event.
  final String event_template =
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

  private String supervisorId;
  private ZkClient zkClient;
  private Boolean segmentsExist;   // to tell if we should remove segments during teardown

  // format for the querying interval
  private final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  private final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
  private DateTime dtFirst;                // timestamp of 1st event
  private DateTime dtLast;                 // timestamp of last event

  @Inject
  private TestQueryHelper queryHelper;
  @Inject
  private IntegrationTestingConfig config;

  @Test
  public void testKafka()
  {
    LOG.info("Starting test: ITKafkaIndexingServiceTest");

    // create topic
    try {
      int sessionTimeoutMs = 10000;
      int connectionTimeoutMs = 10000;
      String zkHosts = config.getZookeeperHosts();
      zkClient = new ZkClient(
          zkHosts, sessionTimeoutMs, connectionTimeoutMs,
          ZKStringSerializer$.MODULE$
      );
      int numPartitions = 4;
      int replicationFactor = 1;
      Properties topicConfig = new Properties();
      AdminUtils.createTopic(zkClient, TOPIC_NAME, numPartitions, replicationFactor, topicConfig);
    }
    catch (TopicExistsException e) {
      // it's ok if the topic already exists
    }
    catch (Exception e) {
      throw new ISE(e, "could not create kafka topic");
    }

    String spec;
    try {
      LOG.info("supervisorSpec name: [%s]", INDEXER_FILE);
      spec = getTaskAsString(INDEXER_FILE)
          .replaceAll("%%DATASOURCE%%", DATASOURCE)
          .replaceAll("%%TOPIC%%", TOPIC_NAME)
          .replaceAll("%%KAFKA_BROKER%%", config.getKafkaHost());
      LOG.info("supervisorSpec: [%s]\n", spec);
    }
    catch (Exception e) {
      LOG.error("could not read file [%s]", INDEXER_FILE);
      throw new ISE(e, "could not read file [%s]", INDEXER_FILE);
    }

    // start supervisor
    supervisorId = indexer.submitSupervisor(spec);
    LOG.info("Submitted supervisor");

    // set up kafka producer
    Properties properties = new Properties();
    properties.put("bootstrap.servers", config.getKafkaHost());
    LOG.info("Kafka bootstrap.servers: [%s]", config.getKafkaHost());
    properties.put("acks", "all");
    properties.put("retries", "3");

    KafkaProducer<String, String> producer = new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new StringSerializer()
    );

    DateTimeZone zone = DateTimeZone.forID("UTC");
    // format for putting into events
    DateTimeFormatter event_fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    DateTime dt = new DateTime(zone); // timestamp to put on events
    dtFirst = dt;            // timestamp of 1st event
    dtLast = dt;             // timestamp of last event
    // stop sending events when time passes this
    DateTime dtStop = dtFirst.plusMinutes(MINUTES_TO_SEND).plusSeconds(30);

    // these are used to compute the expected aggregations
    int added = 0;
    int num_events = 0;

    // send data to kafka
    while (dt.compareTo(dtStop) < 0) {  // as long as we're within the time span
      num_events++;
      added += num_events;
      // construct the event to send
      String event = String.format(event_template, event_fmt.print(dt), num_events, 0, num_events);
      LOG.info("sending event: [%s]", event);
      try {
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, event)).get();
      }
      catch (Exception ioe) {
        throw Throwables.propagate(ioe);
      }

      try {
        Thread.sleep(DELAY_BETWEEN_EVENTS_SECS * 1000);
      }
      catch (InterruptedException ex) { /* nothing */ }
      dtLast = dt;
      dt = new DateTime(zone);
    }

    producer.close();

    InputStream is = ITKafkaIndexingServiceTest.class.getResourceAsStream(QUERIES_FILE);
    if (null == is) {
      throw new ISE("could not open query file: %s", QUERIES_FILE);
    }

    // put the timestamps into the query structure
    String query_response_template;
    try {
      query_response_template = IOUtils.toString(is, "UTF-8");
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", QUERIES_FILE);
    }

    String queryStr = query_response_template
        .replaceAll("%%DATASOURCE%%", DATASOURCE)
        .replace("%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst))
        .replace("%%TIMEBOUNDARY_RESPONSE_MAXTIME%%", TIMESTAMP_FMT.print(dtLast))
        .replace("%%TIMEBOUNDARY_RESPONSE_MINTIME%%", TIMESTAMP_FMT.print(dtFirst))
        .replace("%%TIMESERIES_QUERY_START%%", INTERVAL_FMT.print(dtFirst))
        .replace("%%TIMESERIES_QUERY_END%%", INTERVAL_FMT.print(dtFirst.plusMinutes(MINUTES_TO_SEND + 2)))
        .replace("%%TIMESERIES_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst))
        .replace("%%TIMESERIES_ADDED%%", Integer.toString(added))
        .replace("%%TIMESERIES_NUMEVENTS%%", Integer.toString(num_events));

    // this query will probably be answered from the indexing tasks but possibly from 2 historical segments / 2 indexing
    try {
      this.queryHelper.testQueriesFromString(queryStr, 2);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    indexer.shutdownSupervisor(supervisorId);

    // wait for segments to be handed off
    try {
      RetryUtil.retryUntil(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return coordinator.areSegmentsLoaded(DATASOURCE);
            }
          },
          true,
          30000,
          10,
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

  @AfterClass
  public void afterClass() throws Exception
  {
    LOG.info("teardown");

    // delete kafka topic
    AdminUtils.deleteTopic(zkClient, TOPIC_NAME);

    // remove segments
    if (segmentsExist) {
      unloadAndKillData(DATASOURCE);
    }
  }
}
