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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka Firehose base on Kafka 1.0
 */
public class KafkaFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(KafkaFirehoseFactory.class);
  private static final String DEFAULT_MESSAGE_ENCODING = "UTF-8";
  private final Charset charset;

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final String feed;

  @JsonProperty
  @Nullable
  private final String rowDelimiter;

  @JsonProperty
  private final String messageEncoding;

  @JsonCreator
  public KafkaFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("feed") String feed,
      @JsonProperty("rowDelimiter") @Nullable String rowDelimiter,
      @JsonProperty("messageEncoding") @Nullable String messageEncoding
  )
  {
    this.consumerProps = consumerProps;
    this.feed = feed;
    this.rowDelimiter = rowDelimiter;
    this.messageEncoding = messageEncoding == null ? DEFAULT_MESSAGE_ENCODING : messageEncoding;
    this.charset = Charset.forName(this.messageEncoding);
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser, File temporaryDirectory) throws IOException
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
    if (rowDelimiter != null) {
      Preconditions.checkArgument(
          theParser.getClass().equals(StringInputRowParser.class),
          "rowDelimiter is available only for string input"
      );
    }

    final KafkaConsumer<ByteBuffer, ByteBuffer> consumer = new KafkaConsumer<>(
        consumerProps,
        new ByteBufferDeserializer(),
        new ByteBufferDeserializer()
    );
    consumer.subscribe(ImmutableList.of(feed));

    return new Firehose()
    {
      private Iterator<ByteBuffer> iter = null;
      private List<ByteBuffer> bufferList = new ArrayList<>();
      private AtomicBoolean isCommited = new AtomicBoolean(false);
      private CharBuffer chars = null;

      /**
       * Transform {@code byteBuffer} to String using the charset.<p>
       * Codes are copied from {@link StringInputRowParser} {@code buildStringKeyMap} funtion.
       *
       * @param byteBuffer  the {@link ByteBuffer} to be transformed
       * @return transformed string. return null if failed with {@link CoderResult}
       */
      @Nullable
      private String getString(ByteBuffer byteBuffer)
      {
        int payloadSize = byteBuffer.remaining();

        if (chars == null || chars.remaining() < payloadSize) {
          chars = CharBuffer.allocate(payloadSize);
        }

        final CoderResult coderResult = charset.newDecoder()
                                               .onMalformedInput(CodingErrorAction.REPLACE)
                                               .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                               .decode(byteBuffer, chars, true);
        if (coderResult.isUnderflow()) {
          chars.flip();
          try {
            return chars.toString();
          }
          finally {
            chars.clear();
          }
        } else {
          // Failed with CoderResult
          return null;
        }
      }

      @Override
      public boolean hasMore()
      {
        if (isCommited.get()) {
          // do commit here
          consumer.commitSync();
          log.info("offsets committed.");
          isCommited.set(false);
        }
        try {
          if (iter == null || !iter.hasNext()) {
            bufferList.clear();

            // Always wait for results
            ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
              // For each ConsumerRecord, its value is the message we want
              if (rowDelimiter == null) {
                bufferList.add(record.value());
              } else {
                String recordString = getString(record.value());
                if (recordString != null) {
                  String[] split = StringUtils.split(recordString, rowDelimiter);
                  for (String eachRecordString : split) {
                    bufferList.add(ByteBuffer.wrap(eachRecordString.getBytes(charset)));
                  }
                }
              }
            }
            iter = bufferList.iterator();
          }
        }
        catch (InterruptException e) {
          /*
             If the process is killed, InterruptException will be thrown. It is good to return false.
             But it may not to commit sucessfully. Need a better solution.
            */
          log.error(e, "Polling message interrupted.");
          consumer.commitSync();
          return false;
        }
        return iter.hasNext();
      }

      @Override
      public InputRow nextRow()
      {
        return theParser.parse(iter.next());
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
               KafkaConsumer is not thead safe, so we have to let the thread which polled messages do commit.
             */
            log.info("committing offsets");
            isCommited.set(true);
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        consumer.close();
      }
    };
  }
}
