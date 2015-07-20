/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.firehose.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Committer;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.firehose.kafka.KafkaSimpleConsumer.BytesMessageWithOffset;

public class KafkaEightSimpleConsumerFirehoseFactory implements
    FirehoseFactoryV2<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(
      KafkaEightSimpleConsumerFirehoseFactory.class
  );

  @JsonProperty
  private final List<String> brokerList;

  @JsonProperty
  private final List<Integer> partitionIdList;

  @JsonProperty
  private final String clientId;

  @JsonProperty
  private final String feed;

  @JsonProperty
  private final int queueBufferLength;

  @JsonProperty
  private boolean earliest;

  private final List<PartitionConsumerWorker> consumerWorkers = new CopyOnWriteArrayList<>();
  private static final int DEFAULT_QUEUE_BUFFER_LENGTH = 20000;
  private static final String RESET_TO_LATEST = "latest";
  private static final int CONSUMER_FETCH_TIMEOUT = 10000;
  @JsonCreator
  public KafkaEightSimpleConsumerFirehoseFactory(
      @JsonProperty("brokerList") List<String> brokerList,
      @JsonProperty("partitionIdList") List<Integer> partitionIdList,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("feed") String feed,
      @JsonProperty("queueBufferLength") Integer queueBufferLength,
      @JsonProperty("resetBehavior") String resetBehavior
  )
  {
    this.brokerList = brokerList;
    this.partitionIdList = partitionIdList;
    this.clientId = clientId;
    this.feed = feed;

    this.queueBufferLength = queueBufferLength == null ? DEFAULT_QUEUE_BUFFER_LENGTH : queueBufferLength;
    log.info("queueBufferLength loaded as[%s]", this.queueBufferLength);

    this.earliest = RESET_TO_LATEST.equalsIgnoreCase(resetBehavior) ? false : true;
    log.info("Default behavior of cosumer set to earliest? [%s]", this.earliest);
  }

  private Map<Integer, Long> loadOffsetFromPreviousMetaData(Object lastCommit)
  {
    Map<Integer, Long> offsetMap = Maps.newHashMap();
    if (lastCommit == null) {
      return offsetMap;
    }
    if (lastCommit instanceof Map) {
      Map<Object, Object> lastCommitMap = (Map) lastCommit;
      for (Map.Entry<Object, Object> entry : lastCommitMap.entrySet()) {
        try {
          int partitionId = Integer.parseInt(entry.getKey().toString());
          long offset = Long.parseLong(entry.getValue().toString());
          log.debug("Recover last commit information partitionId [%s], offset [%s]", partitionId, offset);
          offsetMap.put(partitionId, offset);
        }
        catch (NumberFormatException e) {
          log.error(e, "Fail to load offset from previous meta data [%s]", entry);
        }
      }
      log.info("Loaded offset map[%s]", offsetMap);
    } else {
      log.error("Unable to cast lastCommit to Map");
    }
    return offsetMap;
  }

  @Override
  public FirehoseV2 connect(final ByteBufferInputRowParser firehoseParser, Object lastCommit) throws IOException
  {
    final Map<Integer, Long> lastOffsets = loadOffsetFromPreviousMetaData(lastCommit);

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
    for (Integer partition : partitionIdList) {
      final KafkaSimpleConsumer kafkaSimpleConsumer = new KafkaSimpleConsumer(
          feed, partition, clientId, brokerList, earliest
      );
      Long startOffset = lastOffsets.get(partition);
      PartitionConsumerWorker worker = new PartitionConsumerWorker(
          feed, kafkaSimpleConsumer, partition, startOffset == null ? 0 : startOffset
      );
      consumerWorkers.add(worker);
    }

    final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue = new LinkedBlockingQueue<BytesMessageWithOffset>(queueBufferLength);
    log.info("Kicking off all consumers");
    for (PartitionConsumerWorker worker : consumerWorkers) {
      worker.go(messageQueue);
    }
    log.info("All consumer started");

    return new FirehoseV2()
    {
      private ConcurrentMap<Integer, Long> lastOffsetPartitions;
      private volatile boolean stop;
      private volatile boolean interrupted;

      private volatile BytesMessageWithOffset msg = null;
      private volatile InputRow row = null;

      {
        lastOffsetPartitions = Maps.newConcurrentMap();
        lastOffsetPartitions.putAll(lastOffsets);
      }

      @Override
      public void start() throws Exception
      {
        nextMessage();
      }

      @Override
      public boolean advance()
      {
        if (stop) {
          return false;
        }

        nextMessage();
        return true;
      }

      private void nextMessage()
      {
        try {
          row = null;
          while (row == null) {
            if (msg != null) {
              lastOffsetPartitions.put(msg.getPartition(), msg.offset());
            }

            msg = messageQueue.take();
            interrupted = false;

            final byte[] message = msg.message();
            row = message == null ? null : theParser.parse(ByteBuffer.wrap(message));
          }
        }
        catch (InterruptedException e) {
          interrupted = true;
          log.info(e, "Interrupted when taken from queue");
        }
      }

      @Override
      public InputRow currRow()
      {
        if (interrupted) {
          return null;
        }
        return row;
      }

      @Override
      public Committer makeCommitter()
      {
        final Map<Integer, Long> offsets = Maps.newHashMap(lastOffsetPartitions);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return offsets;
          }

          @Override
          public void run()
          {

          }
        };
      }

      @Override
      public void close() throws IOException
      {
        log.info("Stopping kafka 0.8 simple firehose");
        stop = true;
        for (PartitionConsumerWorker t : consumerWorkers) {
          t.close();
        }
      }
    };
  }

  private static class PartitionConsumerWorker implements Closeable
  {
    private final String topic;
    private final KafkaSimpleConsumer consumer;
    private final int partitionId;
    private final long startOffset;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private volatile Thread thread = null;

    PartitionConsumerWorker(String topic, KafkaSimpleConsumer consumer, int partitionId, long startOffset)
    {
      this.topic = topic;
      this.consumer = consumer;
      this.partitionId = partitionId;
      this.startOffset = startOffset;
    }

    public void go(final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue) {
      thread = new Thread()
      {
        @Override
        public void run()
        {
          long offset = startOffset;
          log.info("Start running parition[%s], offset[%s]", partitionId, offset);
          try {
            while (!stopped.get()) {
              try {
                Iterable<BytesMessageWithOffset> msgs = consumer.fetch(offset, CONSUMER_FETCH_TIMEOUT);
                int count = 0;
                for (BytesMessageWithOffset msgWithOffset : msgs) {
                  offset = msgWithOffset.offset();
                  messageQueue.put(msgWithOffset);
                  count++;
                }
                log.debug("fetch [%s] msgs for partition [%s] in one time ", count, partitionId);
              }
              catch (InterruptedException e) {
                log.info("Interrupted when fetching data, shutting down.");
                return;
              }
              catch (Exception e) {
                log.error(e, "Exception happened in fetching data, but will continue consuming");
              }
            }
          }
          finally {
            consumer.stop();
          }
        }
      };
      thread.setDaemon(true);
      thread.setName(String.format("kafka-%s-%s", topic, partitionId));
      thread.start();
    }

    @Override
    public synchronized void close() throws IOException
    {
      if (stopped.compareAndSet(false, true)) {
        thread.interrupt();
        thread = null;
      }
    }
  }
}
