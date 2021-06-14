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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class PulsarRecordSupplier implements RecordSupplier<Integer, String, PulsarRecordEntity>
{
  protected final ConcurrentHashMap<StreamPartition<Integer>, Container> readers = new ConcurrentHashMap<>();
  protected final PulsarClient client;
  protected PulsarClientException previousSeekFailure;

  protected final String readerName;
  protected final String serviceUrl;
  protected final String authPluginClassName;
  protected final String authParams;
  protected final Long operationTimeoutMs;
  protected final Long statsIntervalSeconds;
  protected final Integer numIoThreads;
  protected final Integer numListenerThreads;
  protected final Boolean useTcpNoDelay;
  protected final Boolean useTls;
  protected final String tlsTrustCertsFilePath;
  protected final Boolean tlsAllowInsecureConnection;
  protected final Boolean tlsHostnameVerificationEnable;
  protected final Integer concurrentLookupRequest;
  protected final Integer maxLookupRequest;
  protected final Integer maxNumberOfRejectedRequestPerConnection;
  protected final Integer keepAliveIntervalSeconds;
  protected final Integer connectionTimeoutMs;
  protected final Integer requestTimeoutMs;
  protected final Long maxBackoffIntervalNanos;


  public PulsarRecordSupplier(String readerName,
                              String serviceUrl,
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
    this.readerName = readerName;
    this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "serviceUrl");
    this.authPluginClassName = authPluginClassName;
    this.authParams = authParams;
    this.operationTimeoutMs = operationTimeoutMs;
    this.statsIntervalSeconds = statsIntervalSeconds;
    this.numIoThreads = numIoThreads;
    this.numListenerThreads = numListenerThreads;
    this.useTcpNoDelay = useTcpNoDelay;
    this.useTls = useTls;
    this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
    this.concurrentLookupRequest = concurrentLookupRequest;
    this.maxLookupRequest = maxLookupRequest;
    this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
    this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxBackoffIntervalNanos = maxBackoffIntervalNanos;

    try {
      client = new PulsarClientImpl(createClientConf());
    }
    catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    List<CompletableFuture<Reader<byte[]>>> futures = new ArrayList<>();

    try {
      for (StreamPartition<Integer> partition : streamPartitions) {
        if (readers.containsKey(partition)) {
          continue;
        }

        String topic = TopicName.get(partition.getStream())
                                .getPartition(partition.getPartitionId())
                                .toString();

        futures.add(buildConsumer(client, topic).thenApplyAsync(reader -> {
          if (readers.containsKey(partition)) {
            reader.closeAsync();
          } else {
            readers.put(partition, new Container(reader, MessageId.earliest));
          }
          return reader;
        }));
      }

      futures.forEach(CompletableFuture::join);
    }
    catch (Exception e) {
      futures.forEach(f -> {
        try {
          f.get().closeAsync();
        }
        catch (Exception ignored) {
          // ignore
        }
      });
      throw new StreamException(e);
    }
  }

  public PulsarClientException getPreviousSeekFailure()
  {
    return previousSeekFailure;
  }

  @Override
  public void seek(StreamPartition<Integer> partition, String sequenceNumber) throws InterruptedException
  {
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }

    try {
      final MessageId messageId = PulsarSequenceNumber.of(sequenceNumber).getMessageId();
      reader.reader.seek(PulsarSequenceNumber.of(sequenceNumber).getMessageId());
      setPosition(partition, messageId);
      previousSeekFailure = null;
    }
    catch (PulsarClientException e) {
      previousSeekFailure = e;
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.EARLIEST_OFFSET);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.LATEST_OFFSET);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public abstract List<OrderedPartitionableRecord<Integer, String, PulsarRecordEntity>> poll(long timeout);

  @Override
  public Collection<StreamPartition<Integer>> getAssignment()
  {
    return this.readers.keySet();
  }

  @Nullable
  @Override
  public String getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.LATEST_OFFSET;
  }

  @Nullable
  @Override
  public String getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.EARLIEST_OFFSET;
  }

  @Override
  public String getPosition(StreamPartition<Integer> partition)
  {
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }
    return PulsarSequenceNumber.of(reader.position).get();
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    try {
      return client.getPartitionsForTopic(stream).get().stream()
                   .map(TopicName::get)
                   .map(TopicName::getPartitionIndex)
                   .collect(Collectors.toSet());
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void close()
  {
    readers.forEach((k, r) -> r.reader.closeAsync());
    client.closeAsync();
  }

  protected void setPosition(StreamPartition<Integer> partition, MessageId position)
  {
    Container reader = this.readers.get(partition);
    if (reader != null) {
      reader.position = position;
    }
  }

  protected abstract CompletableFuture<Reader<byte[]>> buildConsumer(PulsarClient client, String topic);

  private ClientConfigurationData createClientConf()
  {
    ClientConfigurationData clientConf = new ClientConfigurationData();
    clientConf.setServiceUrl(this.serviceUrl);
    if (this.authPluginClassName != null) {
      clientConf.setAuthPluginClassName(this.authPluginClassName);
    }
    if (this.authParams != null) {
      clientConf.setAuthParams(this.authParams);
    }
    if (this.operationTimeoutMs != null) {
      clientConf.setOperationTimeoutMs(this.operationTimeoutMs);
    }
    if (this.statsIntervalSeconds != null) {
      clientConf.setStatsIntervalSeconds(this.statsIntervalSeconds);
    }
    if (this.numIoThreads != null) {
      clientConf.setNumIoThreads(this.numIoThreads);
    }
    if (this.numListenerThreads != null) {
      clientConf.setNumListenerThreads(this.numListenerThreads);
    }
    if (this.useTcpNoDelay != null) {
      clientConf.setUseTcpNoDelay(this.useTcpNoDelay);
    }
    if (this.useTls != null) {
      clientConf.setUseTls(this.useTls);
    }
    if (this.tlsTrustCertsFilePath != null) {
      clientConf.setTlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
    }
    if (this.tlsAllowInsecureConnection != null) {
      clientConf.setTlsAllowInsecureConnection(this.tlsAllowInsecureConnection);
    }
    if (this.tlsHostnameVerificationEnable != null) {
      clientConf.setTlsHostnameVerificationEnable(this.tlsHostnameVerificationEnable);
    }
    if (this.concurrentLookupRequest != null) {
      clientConf.setConcurrentLookupRequest(this.concurrentLookupRequest);
    }
    if (this.maxLookupRequest != null) {
      clientConf.setMaxLookupRequest(this.maxLookupRequest);
    }
    if (this.maxNumberOfRejectedRequestPerConnection != null) {
      clientConf.setMaxNumberOfRejectedRequestPerConnection(this.maxNumberOfRejectedRequestPerConnection);
    }
    if (this.keepAliveIntervalSeconds != null) {
      clientConf.setKeepAliveIntervalSeconds(this.keepAliveIntervalSeconds);
    }
    if (this.connectionTimeoutMs != null) {
      clientConf.setConnectionTimeoutMs(this.connectionTimeoutMs);
    }
    if (this.requestTimeoutMs != null) {
      clientConf.setRequestTimeoutMs(this.requestTimeoutMs);
    }
    if (this.maxBackoffIntervalNanos != null) {
      clientConf.setMaxBackoffIntervalNanos(this.maxBackoffIntervalNanos);
    }
    return clientConf;
  }

  public static class Container
  {
    public Reader<byte[]> reader;
    public MessageId position;

    public Container(Reader<byte[]> reader, MessageId position)
    {
      this.reader = reader;
      this.position = position;
    }
  }
}
