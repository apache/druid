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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.logger.Logger;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * refer @{link https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example}
 * <p>
 * This class is not thread safe, the caller must ensure all the methods be
 * called from single thread
 */
public class KafkaSimpleConsumer
{

  public static final List<BytesMessageWithOffset> EMPTY_MSGS = new ArrayList<>();

  private static final Logger log = new Logger(KafkaSimpleConsumer.class);

  private final List<HostAndPort> allBrokers;
  private final String topic;
  private final int partitionId;
  private final String clientId;
  private final String leaderLookupClientId;
  private final boolean earliest;

  private volatile Broker leaderBroker;
  private List<HostAndPort> replicaBrokers;
  private SimpleConsumer consumer = null;

  private static final int SO_TIMEOUT = 30000;
  private static final int BUFFER_SIZE = 65536;
  private static final long RETRY_INTERVAL = 1000L;
  private static final int FETCH_SIZE = 100000000;

  public KafkaSimpleConsumer(String topic, int partitionId, String clientId, List<String> brokers, boolean earliest)
  {
    List<HostAndPort> brokerList = new ArrayList<>();
    for (String broker : brokers) {
      HostAndPort brokerHostAndPort = HostAndPort.fromString(broker);
      Preconditions.checkArgument(
          brokerHostAndPort.getHostText() != null &&
          !brokerHostAndPort.getHostText().isEmpty() &&
          brokerHostAndPort.hasPort(),
          "kafka broker [%s] is not valid, must be <host>:<port>",
          broker
      );
      brokerList.add(brokerHostAndPort);
    }

    this.allBrokers = Collections.unmodifiableList(brokerList);
    this.topic = topic;
    this.partitionId = partitionId;
    this.clientId = StringUtils.format("%s_%d_%s", topic, partitionId, clientId);
    this.leaderLookupClientId = clientId + "leaderLookup";
    this.replicaBrokers = new ArrayList<>();
    this.replicaBrokers.addAll(this.allBrokers);
    this.earliest = earliest;
    log.info(
        "KafkaSimpleConsumer initialized with clientId [%s] for message consumption and clientId [%s] for leader lookup",
        this.clientId,
        this.leaderLookupClientId
    );
  }

  private void ensureConsumer(Broker leader) throws InterruptedException
  {
    if (consumer == null) {
      while (leaderBroker == null) {
        leaderBroker = findNewLeader(leader);
      }
      log.info(
          "making SimpleConsumer[%s][%s], leader broker[%s:%s]",
          topic, partitionId, leaderBroker.host(), leaderBroker.port()
      );

      consumer = new SimpleConsumer(
          leaderBroker.host(), leaderBroker.port(), SO_TIMEOUT, BUFFER_SIZE, clientId
      );
    }
  }

  public static class BytesMessageWithOffset
  {
    final byte[] msg;
    final long offset;
    final int partition;

    public BytesMessageWithOffset(byte[] msg, long offset, int partition)
    {
      this.msg = msg;
      this.offset = offset;
      this.partition = partition;
    }

    public int getPartition()
    {
      return partition;
    }

    public byte[] message()
    {
      return msg;
    }

    public long offset()
    {
      return offset;
    }
  }

  private Iterable<BytesMessageWithOffset> filterAndDecode(Iterable<MessageAndOffset> kafkaMessages, final long offset)
  {
    return FunctionalIterable
        .create(kafkaMessages)
        .filter(
            new Predicate<MessageAndOffset>()
            {
              @Override
              public boolean apply(MessageAndOffset msgAndOffset)
              {
                return msgAndOffset.offset() >= offset;
              }
            }
        )
        .transform(
            new Function<MessageAndOffset, BytesMessageWithOffset>()
            {

              @Override
              public BytesMessageWithOffset apply(MessageAndOffset msgAndOffset)
              {
                ByteBuffer bb = msgAndOffset.message().payload();
                byte[] payload = new byte[bb.remaining()];
                bb.get(payload);
                // add nextOffset here, thus next fetch will use nextOffset instead of current offset
                return new BytesMessageWithOffset(payload, msgAndOffset.nextOffset(), partitionId);
              }
            }
        );
  }

  private long getOffset(boolean earliest) throws InterruptedException
  {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(
        topicAndPartition,
        new PartitionOffsetRequestInfo(
            earliest ? kafka.api.OffsetRequest.EarliestTime() : kafka.api.OffsetRequest.LatestTime(), 1
        )
    );
    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId
    );
    OffsetResponse response = null;
    try {
      response = consumer.getOffsetsBefore(request);
    }
    catch (Exception e) {
      ensureNotInterrupted(e);
      log.error(e, "caught exception in getOffsetsBefore [%s] - [%s]", topic, partitionId);
      return -1;
    }
    if (response.hasError()) {
      log.error(
          "error fetching data Offset from the Broker [%s]. reason: [%s]", leaderBroker.host(),
          response.errorCode(topic, partitionId)
      );
      return -1;
    }
    long[] offsets = response.offsets(topic, partitionId);
    return earliest ? offsets[0] : offsets[offsets.length - 1];
  }

  public Iterable<BytesMessageWithOffset> fetch(long offset, int timeoutMs) throws InterruptedException
  {
    FetchResponse response = null;
    Broker previousLeader = leaderBroker;
    while (true) {
      ensureConsumer(previousLeader);

      FetchRequest request = new FetchRequestBuilder()
          .clientId(clientId)
          .addFetch(topic, partitionId, offset, FETCH_SIZE)
          .maxWait(timeoutMs)
          .minBytes(1)
          .build();

      log.debug("fetch offset %s", offset);

      try {
        response = consumer.fetch(request);
      }
      catch (Exception e) {
        ensureNotInterrupted(e);
        log.warn(e, "caught exception in fetch %s - %d", topic, partitionId);
        response = null;
      }

      if (response == null || response.hasError()) {
        short errorCode = response != null ? response.errorCode(topic, partitionId) : ErrorMapping.UnknownCode();
        log.warn("fetch %s - %s with offset %s encounters error: [%s]", topic, partitionId, offset, errorCode);

        boolean needNewLeader = false;
        if (errorCode == ErrorMapping.RequestTimedOutCode()) {
          log.info("kafka request timed out, response[%s]", response);
        } else if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
          long newOffset = getOffset(earliest);
          log.info("got [%s] offset[%s] for [%s][%s]", earliest ? "earliest" : "latest", newOffset, topic, partitionId);
          if (newOffset < 0) {
            needNewLeader = true;
          } else {
            offset = newOffset;
            continue;
          }
        } else {
          needNewLeader = true;
        }

        if (needNewLeader) {
          stopConsumer();
          previousLeader = leaderBroker;
          leaderBroker = null;
          continue;
        }
      } else {
        break;
      }
    }

    return response != null ? filterAndDecode(response.messageSet(topic, partitionId), offset) : EMPTY_MSGS;
  }

  private void stopConsumer()
  {
    if (consumer != null) {
      try {
        consumer.close();
        log.info("stop consumer[%s][%s], leaderBroker[%s]", topic, partitionId, leaderBroker);
      }
      catch (Exception e) {
        log.warn(e, "stop consumer[%s][%s] failed", topic, partitionId);
      }
      finally {
        consumer = null;
      }
    }
  }

  public void stop()
  {
    stopConsumer();
    log.info("KafkaSimpleConsumer[%s][%s] stopped", topic, partitionId);
  }

  private PartitionMetadata findLeader() throws InterruptedException
  {
    for (HostAndPort broker : replicaBrokers) {
      SimpleConsumer consumer = null;
      try {
        log.info("Finding new leader from Kafka brokers, try broker [%s]", broker.toString());
        consumer = new SimpleConsumer(broker.getHostText(), broker.getPort(), SO_TIMEOUT, BUFFER_SIZE, leaderLookupClientId);
        TopicMetadataResponse resp = consumer.send(new TopicMetadataRequest(Collections.singletonList(topic)));

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          if (topic.equals(item.topic())) {
            for (PartitionMetadata part : item.partitionsMetadata()) {
              if (part.partitionId() == partitionId) {
                return part;
              }
            }
          }
        }
      }
      catch (Exception e) {
        ensureNotInterrupted(e);
        log.warn(
            e, "error communicating with Kafka Broker [%s] to find leader for [%s] - [%s]", broker, topic, partitionId
        );
      }
      finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    return null;
  }

  private Broker findNewLeader(Broker oldLeader) throws InterruptedException
  {
    long retryCnt = 0;
    while (true) {
      PartitionMetadata metadata = findLeader();
      if (metadata != null) {
        replicaBrokers.clear();
        for (Broker replica : metadata.replicas()) {
          replicaBrokers.add(
              HostAndPort.fromParts(replica.host(), replica.port())
          );
        }

        log.debug("Got new Kafka leader metadata : [%s], previous leader : [%s]", metadata, oldLeader);
        Broker newLeader = metadata.leader();
        if (newLeader != null) {
          // We check the retryCnt here as well to make sure that we have slept a little bit
          // if we don't notice a change in leadership
          // just in case if Zookeeper doesn't get updated fast enough
          if (oldLeader == null || isValidNewLeader(newLeader) || retryCnt != 0) {
            return newLeader;
          }
        }
      }

      Thread.sleep(RETRY_INTERVAL);
      retryCnt++;
      // if could not find the leader for current replicaBrokers, let's try to
      // find one via allBrokers
      if (retryCnt >= 3 && (retryCnt - 3) % 5 == 0) {
        log.warn("cannot find leader for [%s] - [%s] after [%s] retries", topic, partitionId, retryCnt);
        replicaBrokers.clear();
        replicaBrokers.addAll(allBrokers);
      }
    }
  }

  private boolean isValidNewLeader(Broker broker)
  {
    // broker is considered valid new leader if it is not the same as old leaderBroker
    return !(leaderBroker.host().equalsIgnoreCase(broker.host()) && leaderBroker.port() == broker.port());
  }

  private void ensureNotInterrupted(Exception e) throws InterruptedException
  {
    if (Thread.interrupted()) {
      log.error(e, "Interrupted during fetching for %s - %s", topic, partitionId);
      throw new InterruptedException();
    }
  }
}
