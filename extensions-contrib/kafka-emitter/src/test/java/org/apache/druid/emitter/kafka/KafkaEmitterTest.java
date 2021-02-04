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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.DefaultRequestLogEventBuilderFactory;
import org.apache.druid.server.log.RequestLogEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class KafkaEmitterTest
{

  private static final String METRIC_TOPIC_NAME = "metricTest";
  private static final String ALERT_TOPIC_NAME = "alertTest";
  private static final String REQUEST_SQL_TOPIC_NAME = "requestSqlTest";

  @ClassRule
  public static final SharedKafkaTestResource SHARED_KAFKA_TEST_RESOURCE = new SharedKafkaTestResource()
      .withBrokers(1).registerListener(new PlainListener().onPorts(9092));
  private ObjectMapper mapper = new DefaultObjectMapper();
  private KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("localhost:9092",
                                                                         METRIC_TOPIC_NAME,
                                                                         ALERT_TOPIC_NAME,
                                                                         REQUEST_SQL_TOPIC_NAME,
                                                                         "clusterNameTest",
                                                                         null
  );

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
    getKafkaTestUtils().createTopic(METRIC_TOPIC_NAME, 1, (short) 1);
    getKafkaTestUtils().createTopic(ALERT_TOPIC_NAME, 1, (short) 1);
    getKafkaTestUtils().createTopic(REQUEST_SQL_TOPIC_NAME, 1, (short) 1);
  }

  @Test
  public void testServiceMetricEventKafkaEmitter() throws Exception
  {
    KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();
    KafkaEmitter kafkaEmitter = new KafkaEmitter(kafkaEmitterConfig, mapper);
    kafkaEmitter.start();
    // sleep and wait kafka producer startup
    Thread.sleep(30000);

    ServiceMetricEvent serviceMetricEvent = new ServiceMetricEvent.Builder().setDimension("testKey", "testValue")
                                                                            .build(DateTimes.nowUtc(), "query/time", new Double(1))
                                                                            .build("druid/broker", "localhost:8082");
    kafkaEmitter.emit(serviceMetricEvent);
    kafkaEmitter.flush();
    Thread.sleep(5000);

    List<ConsumerRecord<String, String>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(
        METRIC_TOPIC_NAME,
        StringDeserializer.class,
        StringDeserializer.class
    );
    Assert.assertNotNull(consumerRecords);
    Assert.assertEquals(1, consumerRecords.size());
  }

  @Test
  public void testDefaultRequestLogEventKafkaEmitter() throws Exception
  {
    KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();
    KafkaEmitter kafkaEmitter = new KafkaEmitter(kafkaEmitterConfig, mapper);
    kafkaEmitter.start();
    // sleep and wait kafka producer startup
    Thread.sleep(30000);

    Query query = Druids.newScanQueryBuilder()
                        .dataSource("dual")
                        .intervals(new SpecificSegmentSpec(new SegmentDescriptor(Intervals.utc(DateTimes.nowUtc().getMillis(), DateTimes.nowUtc().getMillis() + 60000),
                                                                                 "version", 1)))
                        .build();
    RequestLogLine requestLogLine = RequestLogLine.forNative(query,
                                                             DateTimes.nowUtc(),
                                                             "localhost:8082",
                                                             new QueryStats(ImmutableMap.of()));
    RequestLogEvent requestLogEvent = DefaultRequestLogEventBuilderFactory.instance()
                                                                          .createRequestLogEventBuilder("metric", requestLogLine)
                                                                          .build(ImmutableMap.of());
    kafkaEmitter.emit(requestLogEvent);
    kafkaEmitter.flush();
    Thread.sleep(5000);

    List<ConsumerRecord<String, String>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(
        REQUEST_SQL_TOPIC_NAME,
        StringDeserializer.class,
        StringDeserializer.class
    );
    Assert.assertNotNull(consumerRecords);
    Assert.assertEquals(0, consumerRecords.size());

    Map<String, Object> sqlQueryContext = ImmutableMap.<String, Object>builder().put("sqlQueryId", "1").build();
    requestLogLine = RequestLogLine.forSql(
        "select * from dual",
        sqlQueryContext,
        DateTimes.nowUtc(),
        "localhost:8082",
        new QueryStats(ImmutableMap.<String, Object>builder()
                           .put("sqlQuery/time", 1)
                           .put("sqlQuery/bytes", 1)
                           .build())
    );
    requestLogEvent = DefaultRequestLogEventBuilderFactory.instance()
                                                          .createRequestLogEventBuilder("metric", requestLogLine)
                                                          .build("druid/broker", "localhost:8082");
    kafkaEmitter.emit(requestLogEvent);
    kafkaEmitter.flush();
    Thread.sleep(5000);

    consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(
        REQUEST_SQL_TOPIC_NAME,
        StringDeserializer.class,
        StringDeserializer.class
    );
    Assert.assertNotNull(consumerRecords);
    Assert.assertEquals(1, consumerRecords.size());

  }

  @Test
  public void testAlertEventKafkaEmitter() throws Exception
  {
    KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();
    KafkaEmitter kafkaEmitter = new KafkaEmitter(kafkaEmitterConfig, mapper);
    kafkaEmitter.start();
    // sleep and wait kafka producer startup
    Thread.sleep(30000);

    AlertEvent alertEvent = new AlertEvent("druid/broker", "localhost:8082", "test alert event");
    kafkaEmitter.emit(alertEvent);
    kafkaEmitter.flush();
    Thread.sleep(5000);

    List<ConsumerRecord<String, String>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(
        ALERT_TOPIC_NAME,
        StringDeserializer.class,
        StringDeserializer.class
    );
    Assert.assertNotNull(consumerRecords);
    Assert.assertEquals(1, consumerRecords.size());
  }

  private KafkaTestUtils getKafkaTestUtils()
  {
    return SHARED_KAFKA_TEST_RESOURCE.getKafkaTestUtils();
  }
}
