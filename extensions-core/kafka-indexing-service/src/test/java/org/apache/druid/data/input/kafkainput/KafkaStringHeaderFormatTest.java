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

package org.apache.druid.data.input.kafkainput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;


public class KafkaStringHeaderFormatTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final KafkaStringHeaderFormat KAFKAHEADERNOENCODE = new KafkaStringHeaderFormat(null);
  private static final Iterable<Header> SAMPLE_HEADERS = ImmutableList.of(
      new Header()
      {
        @Override
        public String key()
        {
          return "encoding";
        }

        @Override
        public byte[] value()
        {
          return "application/json".getBytes(StandardCharsets.UTF_8);
        }
      },
      new Header()
      {
        @Override
        public String key()
        {
          return "kafkapkc";
        }

        @Override
        public byte[] value()
        {
          return "pkc-bar".getBytes(StandardCharsets.UTF_8);
        }
      }
  );
  private KafkaRecordEntity inputEntity;
  private long timestamp = DateTimes.of("2021-06-24T00:00:00.000Z").getMillis();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Assert.assertEquals(
        KAFKAHEADERNOENCODE,
        KAFKAHEADERNOENCODE
    );
    Assert.assertEquals(
        KAFKAHEADERNOENCODE,
        MAPPER.readValue(MAPPER.writeValueAsString(KAFKAHEADERNOENCODE), KafkaStringHeaderFormat.class)
    );
    final KafkaStringHeaderFormat kafkaAsciiHeader = new KafkaStringHeaderFormat("US-ASCII");
    Assert.assertNotEquals(
        KAFKAHEADERNOENCODE,
        kafkaAsciiHeader
    );
  }

  @Test
  public void testDefaultHeaderFormat()
  {
    String headerLabelPrefix = "test.kafka.header.";
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
        "sample", 0, 0, timestamp,
        null, null, 0, 0,
        null, "sampleValue".getBytes(StandardCharsets.UTF_8), headers
    ));
    List<Pair<String, Object>> expectedResults = Arrays.asList(
        Pair.of("test.kafka.header.encoding", "application/json"),
        Pair.of("test.kafka.header.kafkapkc", "pkc-bar")
    );

    KafkaHeaderFormat headerInput = new KafkaStringHeaderFormat(null);
    KafkaHeaderReader headerParser = headerInput.createReader(inputEntity.getRecord().headers(), headerLabelPrefix);
    Assert.assertEquals(expectedResults, headerParser.read());
  }

  @Test
  public void testASCIIHeaderFormat()
  {
    Iterable<Header> header = ImmutableList.of(
        new Header()
        {
          @Override
          public String key()
          {
            return "encoding";
          }

          @Override
          public byte[] value()
          {
            return "application/json".getBytes(StandardCharsets.US_ASCII);
          }
        },
        new Header()
        {
          @Override
          public String key()
          {
            return "kafkapkc";
          }

          @Override
          public byte[] value()
          {
            return "pkc-bar".getBytes(StandardCharsets.US_ASCII);
          }
        }
    );

    String headerLabelPrefix = "test.kafka.header.";
    Headers headers = new RecordHeaders(header);
    inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
        "sample", 0, 0, timestamp,
        null, null, 0, 0,
        null, "sampleValue".getBytes(StandardCharsets.UTF_8), headers
    ));
    List<Pair<String, Object>> expectedResults = Arrays.asList(
        Pair.of("test.kafka.header.encoding", "application/json"),
        Pair.of("test.kafka.header.kafkapkc", "pkc-bar")
    );

    KafkaHeaderFormat headerInput = new KafkaStringHeaderFormat("US-ASCII");
    KafkaHeaderReader headerParser = headerInput.createReader(inputEntity.getRecord().headers(), headerLabelPrefix);
    List<Pair<String, Object>> rows = headerParser.read();
    Assert.assertEquals(expectedResults, rows);
  }

  @Test
  public void testIllegalHeaderCharacter()
  {
    Iterable<Header> header = ImmutableList.of(
        new Header()
        {
          @Override
          public String key()
          {
            return "encoding";
          }

          @Override
          public byte[] value()
          {
            return "€pplic€tion/json".getBytes(StandardCharsets.US_ASCII);
          }
        },
        new Header()
        {
          @Override
          public String key()
          {
            return "kafkapkc";
          }

          @Override
          public byte[] value()
          {
            return "pkc-bar".getBytes(StandardCharsets.US_ASCII);
          }
        }
    );

    String headerLabelPrefix = "test.kafka.header.";
    Headers headers = new RecordHeaders(header);
    inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
        "sample", 0, 0, timestamp,
        null, null, 0, 0,
        null, "sampleValue".getBytes(StandardCharsets.UTF_8), headers
    ));
    List<Pair<String, Object>> expectedResults = Arrays.asList(
        Pair.of("test.kafka.header.encoding", "?pplic?tion/json"),
        Pair.of("test.kafka.header.kafkapkc", "pkc-bar")
    );

    KafkaHeaderFormat headerInput = new KafkaStringHeaderFormat("US-ASCII");
    KafkaHeaderReader headerParser = headerInput.createReader(inputEntity.getRecord().headers(), headerLabelPrefix);
    List<Pair<String, Object>> rows = headerParser.read();
    Assert.assertEquals(expectedResults, rows);
  }
}

