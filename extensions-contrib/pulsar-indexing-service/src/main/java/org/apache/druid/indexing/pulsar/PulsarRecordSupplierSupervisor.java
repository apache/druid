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

package org.apache.druid.indexing.pulsar;

import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PulsarRecordSupplierSupervisor extends PulsarRecordSupplier
{
  // TODO(jpg): Determine if this should be unique per supervisor
  private static final String TASK_ID = "druid-pulsar-indexing-service-supervisor";
  private final ConcurrentHashMap<StreamPartition<Integer>, MessageId> positions = new ConcurrentHashMap<>();

  public PulsarRecordSupplierSupervisor(String serviceUrl,
                                        String authPluginClassName,
                                        String authParams,
                                        Long operationTimeoutMs,
                                        Long statsIntervalSeconds,
                                        Integer numIoThreads,
                                        Integer numListenerThreads,
                                        Boolean useTcpNoDelay,
                                        Boolean useTls,
                                        String tlsTrustCertsFilePath,
                                        Boolean tlsAllowInsecureConnection,
                                        Boolean tlsHostnameVerificationEnable,
                                        Integer concurrentLookupRequest,
                                        Integer maxLookupRequest,
                                        Integer maxNumberOfRejectedRequestPerConnection,
                                        Integer keepAliveIntervalSeconds,
                                        Integer connectionTimeoutMs,
                                        Integer requestTimeoutMs,
                                        Long maxBackoffIntervalNanos)
  {
    super(TASK_ID,
          serviceUrl,
          authPluginClassName,
          authParams,
          operationTimeoutMs,
          statsIntervalSeconds,
          numIoThreads,
          numListenerThreads,
          useTcpNoDelay,
          useTls,
          tlsTrustCertsFilePath,
          tlsAllowInsecureConnection,
          tlsHostnameVerificationEnable,
          concurrentLookupRequest,
          maxLookupRequest,
          maxNumberOfRejectedRequestPerConnection,
          keepAliveIntervalSeconds,
          connectionTimeoutMs,
          requestTimeoutMs,
          maxBackoffIntervalNanos);
  }

  @Override
  protected CompletableFuture<Reader<byte[]>> buildConsumer(PulsarClient client, String topic)
  {
    return client.newReader()
                 .readerName(readerName)
                 .topic(topic)
                 .startMessageId(MessageId.latest)
                 .startMessageIdInclusive()
                 .createAsync();
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> positions.put(p, MessageId.latest));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, String sequenceNumber)
  {
    positions.put(partition, PulsarSequenceNumber.of(sequenceNumber).getMessageId());
  }

  private CompletableFuture<MessageId> findEdge(StreamPartition<Integer> partition, Boolean useEarliestMessageId)
  {
    String topic = TopicName.get(partition.getStream()).getPartition(partition.getPartitionId()).toString();
    return client.newReader()
                 .readerName(TASK_ID)
                 .topic(topic)
                 .startMessageId(useEarliestMessageId ? MessageId.earliest : MessageId.latest)
                 .startMessageIdInclusive()
                 .createAsync()
                 .thenApply(reader -> {
                   try {
                     Message<byte[]> msg = reader.readNext(0, TimeUnit.MILLISECONDS);
                     if (msg != null) {
                       return msg.getMessageId();
                     }
                   }
                   catch (PulsarClientException e) {
                     throw new StreamException(e);
                   }
                   finally {
                     reader.closeAsync();
                   }

                   return useEarliestMessageId ? MessageId.earliest : MessageId.latest;
                 });
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
  {
    CompletableFuture.allOf(streamPartitions.stream()
                                            .map(p ->
                                                     findEdge(p, true)
                                                         .thenApply(messageId -> positions.put(p, messageId))
                                            )
                                            .toArray(CompletableFuture[]::new));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
  {
    CompletableFuture.allOf(streamPartitions.stream()
                                            .map(p ->
                                                     findEdge(p, false)
                                                         .thenApply(messageId -> positions.put(p, messageId))
                                            )
                                            .toArray(CompletableFuture[]::new));
  }

  @Override
  public String getPosition(StreamPartition<Integer> partition)
  {
    MessageId position = positions.get(partition);
    if (position == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }
    return PulsarSequenceNumber.of(position).get();
  }

  @NotNull
  @Override
  public List<OrderedPartitionableRecord<Integer, String, PulsarRecordEntity>> poll(long timeout)
  {
    return new ArrayList<>();
  }
}
