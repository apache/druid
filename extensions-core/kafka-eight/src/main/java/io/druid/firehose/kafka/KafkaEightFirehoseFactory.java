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

package io.druid.firehose.kafka;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.InvalidMessageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class KafkaEightFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(KafkaEightFirehoseFactory.class);

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final String feed;

  @JsonCreator
  public KafkaEightFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("feed") String feed

  )
  {
    this.consumerProps = consumerProps;
    this.feed = feed;
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser) throws IOException
  {
    Set<String> newDimExclus = Sets.union(
        firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
        Sets.newHashSet("feed")
    );
    final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
        firehoseParser.getParseSpec()
                      .withDimensionsSpec(
                          firehoseParser.getParseSpec()
                                        .getDimensionsSpec()
                                        .withDimensionExclusions(
                                            newDimExclus
                                        )
                      )
    );

    final ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

    final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(
        ImmutableMap.of(
            feed,
            1
        )
    );

    final List<KafkaStream<byte[], byte[]>> streamList = streams.get(feed);
    if (streamList == null || streamList.size() != 1) {
      return null;
    }

    final KafkaStream<byte[], byte[]> stream = streamList.get(0);
    final ConsumerIterator<byte[], byte[]> iter = stream.iterator();

    return new Firehose()
    {
      @Override
      public boolean hasMore()
      {
        return iter.hasNext();
      }

      @Override
      public InputRow nextRow()
      {
        try {
          final byte[] message = iter.next().message();

          if (message == null) {
            return null;
          }

          return theParser.parse(ByteBuffer.wrap(message));

        }
        catch (InvalidMessageException e) {
          /*
          IF the CRC is caused within the wire transfer, this is not the best way to handel CRC.
          Probably it is better to shutdown the fireHose without commit and start it again.
           */
          log.error(e,"Message failed its checksum and it is corrupt, will skip it");
          return null;
        }
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
               /*
                 This is actually not going to do exactly what we want, cause it will be called asynchronously
                 after the persist is complete.  So, it's going to commit that it's processed more than was actually
                 persisted.  This is unfortunate, but good enough for now.  Should revisit along with an upgrade
                 of our Kafka version.
               */

            log.info("committing offsets");
            connector.commitOffsets();
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        connector.shutdown();
      }
    };
  }

}
