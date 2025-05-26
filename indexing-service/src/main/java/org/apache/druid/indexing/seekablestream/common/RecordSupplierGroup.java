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

package org.apache.druid.indexing.seekablestream.common;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Appears like a single RecordSupplier, except muxes calls to a map of underlying RecordSuppliers.
 * Used in situations where one needs to read from multiple RecordSuppliers at once, e.g multi-cluster ingest.
 */
public class RecordSupplierGroup<PartitionIdType extends PartitionId, SequenceOffsetType, RecordType extends ByteEntity>
    implements Closeable, RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType>
{
  private static final Logger log = new Logger(RecordSupplierGroup.class);
  final ImmutableMap<String, RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType>> suppliers;

  public RecordSupplierGroup(
      final List<String> keyList,
      final List<RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType>> supplierList
  )
  {
    if (keyList.size() != supplierList.size()) {
      throw new IllegalArgumentException("Key list and supplier list must have the same size.");
    }

    final ImmutableMap.Builder<String, RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType>> builder = ImmutableMap.builder();
    for (int i = 0; i < keyList.size(); ++i) {
      builder.put(keyList.get(i), supplierList.get(i));
    }
    this.suppliers = builder.build();
  }

  @Override
  public void assign(Set<StreamPartition<PartitionIdType>> streamPartitions)
  {
    streamPartitions.stream()
                    .collect(Collectors.groupingBy(
                        streamPartition -> streamPartition.getPartitionId().getCluster(),
                        Collectors.toSet()
                    ))
                    .forEach((clusterId, partitions) -> this.suppliers.get(clusterId).assign(partitions));
  }

  @Override
  public void seek(StreamPartition<PartitionIdType> partition, SequenceOffsetType sequenceNumber)
      throws InterruptedException
  {
    this.suppliers.get(partition.getPartitionId().getCluster()).seek(partition, sequenceNumber);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<PartitionIdType>> streamPartitions) throws InterruptedException
  {
    streamPartitions.stream()
                    .collect(Collectors.groupingBy(
                        streamPartition -> streamPartition.getPartitionId().getCluster(),
                        Collectors.toSet()
                    ))
                    .forEach((clusterId, partitions) -> {
                      try {
                        this.suppliers.get(clusterId).seekToEarliest(partitions);
                      }
                      catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<PartitionIdType>> streamPartitions) throws InterruptedException
  {
    streamPartitions.stream()
                    .collect(Collectors.groupingBy(
                        streamPartition -> streamPartition.getPartitionId().getCluster(),
                        Collectors.toSet()
                    ))
                    .forEach((clusterId, partitions) -> {
                      try {
                        this.suppliers.get(clusterId).seekToLatest(partitions);
                      }
                      catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    });
  }

  @Override
  public Collection<StreamPartition<PartitionIdType>> getAssignment()
  {
    return suppliers.values()
                    .stream()
                    .flatMap(supplier -> supplier.getAssignment().stream())
                    .collect(Collectors.toList());
  }

  /**
   * Polls all record suppliers asynchronously with a specified timeout.
   * This is currently only used in sampler specs and not in tasks.
   *
   * @param timeout the maximum time to wait for all suppliers to poll records, in milliseconds.
   * @return a list of records from all suppliers that completed within the timeout. If a supplier fails or times out,
   * its results are not included in the returned list.
   * @throws RuntimeException if an unexpected exception occurs while waiting for the suppliers to complete.
   */
  @Override
  public List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> poll(long timeout)
  {
    final List<CompletableFuture<List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>>>> futures = suppliers.values()
                                                                                                                                        .stream()
                                                                                                                                        .map(
                                                                                                                                            supplier -> CompletableFuture.supplyAsync(
                                                                                                                                                () -> supplier.poll(
                                                                                                                                                    timeout)))
                                                                                                                                        .collect(
                                                                                                                                            Collectors.toList());
    final CompletableFuture<Void> groupFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    try {
      groupFuture.get(timeout, TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      log.warn(e, "Record suppliers poll() timed out");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return futures.stream().map(future -> {
      try {
        return future.getNow(null);
      }
      catch (Exception e) {
        log.error(e, "Failure polling from record supplier");
      }
      return null;
    }).filter(result -> result != null).flatMap(List::stream).collect(Collectors.toList());
  }

  @Override
  public @Nullable SequenceOffsetType getLatestSequenceNumber(StreamPartition<PartitionIdType> partition)
  {
    return suppliers.get(partition.getPartitionId().getCluster()).getLatestSequenceNumber(partition);
  }

  @Override
  public @Nullable SequenceOffsetType getEarliestSequenceNumber(StreamPartition<PartitionIdType> partition)
  {
    return suppliers.get(partition.getPartitionId().getCluster()).getEarliestSequenceNumber(partition);
  }

  @Override
  public boolean isOffsetAvailable(
      StreamPartition<PartitionIdType> partition,
      OrderedSequenceNumber<SequenceOffsetType> offset
  )
  {
    return suppliers.get(partition.getPartitionId().getCluster()).isOffsetAvailable(partition, offset);
  }

  @Override
  public SequenceOffsetType getPosition(StreamPartition<PartitionIdType> partition)
  {
    return suppliers.get(partition.getPartitionId().getCluster()).getPosition(partition);
  }

  @Override
  public Set<PartitionIdType> getPartitionIds(String stream)
  {
    return suppliers.values()
                    .stream()
                    .flatMap(supplier -> supplier.getPartitionIds(stream).stream())
                    .collect(Collectors.toSet());
  }

  @Override
  public void close()
  {
    try {
      for (final RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> supplier : suppliers.values()) {
        supplier.close();
      }
    }
    catch (Exception e) {
      log.error(e, "Failed to close record supplier group");
    }
  }
}
