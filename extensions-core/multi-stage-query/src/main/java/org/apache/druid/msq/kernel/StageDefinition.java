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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Definition of a stage in a multi-stage {@link QueryDefinition}.
 * <p>
 * Each stage has a list of {@link InputSpec} describing its inputs. The position of each spec within the list is
 * its "input number". Some inputs are broadcast to all workers (see {@link #getBroadcastInputNumbers()}). Other,
 * non-broadcast inputs are split up across workers.
 * <p>
 * The number of workers in a stage is at most {@link #getMaxWorkerCount()}. It may be less, depending on the
 * {@link WorkerAssignmentStrategy} in play and depending on the number of distinct inputs available. (For example:
 * if there is only one input file, then there can be only one worker.)
 * <p>
 * Each stage has a {@link FrameProcessorFactory} describing the work it does. Output frames written by these
 * processors have the signature given by {@link #getSignature()}.
 * <p>
 * Each stage has a {@link ShuffleSpec} describing the shuffle that occurs as part of the stage. The shuffle spec is
 * optional: if none is provided, then the {@link FrameProcessorFactory} directly writes to output partitions. If a
 * shuffle spec is provided, then the {@link FrameProcessorFactory} is expected to sort each output frame individually
 * according to {@link ShuffleSpec#clusterBy()}. The execution system handles the rest, including sorting data across
 * frames and producing the appropriate output partitions.
 * <p>
 * The rarely-used parameter {@link #getShuffleCheckHasMultipleValues()} controls whether the execution system
 * checks, while shuffling, if the key used for shuffling has any multi-value fields. When this is true, the method
 * {@link ClusterByStatisticsCollector#hasMultipleValues} is enabled on collectors
 * {@link #createResultKeyStatisticsCollector}. Its primary purpose is to allow ingestion jobs to detect whether the
 * secondary partitioning (CLUSTERED BY) key is multivalued or not.
 */
public class StageDefinition
{
  private static final int MAX_PARTITIONS = 25_000; // Limit for TooManyPartitions

  // If adding any fields here, add them to builder(StageDefinition) below too.
  private final StageId id;
  private final List<InputSpec> inputSpecs;
  private final IntSet broadcastInputNumbers;
  @SuppressWarnings("rawtypes")
  private final FrameProcessorFactory processorFactory;
  private final RowSignature signature;
  private final int maxWorkerCount;
  private final boolean shuffleCheckHasMultipleValues;

  @Nullable
  private final ShuffleSpec shuffleSpec;

  // Set here to encourage sharing, rather than re-creation.
  private final Supplier<FrameReader> frameReader;

  @JsonCreator
  StageDefinition(
      @JsonProperty("id") final StageId id,
      @JsonProperty("input") final List<InputSpec> inputSpecs,
      @JsonProperty("broadcast") final Set<Integer> broadcastInputNumbers,
      @SuppressWarnings("rawtypes") @JsonProperty("processor") final FrameProcessorFactory processorFactory,
      @JsonProperty("signature") final RowSignature signature,
      @Nullable @JsonProperty("shuffleSpec") final ShuffleSpec shuffleSpec,
      @JsonProperty("maxWorkerCount") final int maxWorkerCount,
      @JsonProperty("shuffleCheckHasMultipleValues") final boolean shuffleCheckHasMultipleValues
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.inputSpecs = Preconditions.checkNotNull(inputSpecs, "inputSpecs");

    if (broadcastInputNumbers == null) {
      this.broadcastInputNumbers = IntSets.emptySet();
    } else if (broadcastInputNumbers instanceof IntSet) {
      this.broadcastInputNumbers = (IntSet) broadcastInputNumbers;
    } else {
      this.broadcastInputNumbers = new IntAVLTreeSet(broadcastInputNumbers);
    }

    this.processorFactory = Preconditions.checkNotNull(processorFactory, "processorFactory");
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.shuffleSpec = shuffleSpec;
    this.maxWorkerCount = maxWorkerCount;
    this.shuffleCheckHasMultipleValues = shuffleCheckHasMultipleValues;
    this.frameReader = Suppliers.memoize(() -> FrameReader.create(signature))::get;

    if (mustGatherResultKeyStatistics() && shuffleSpec.clusterBy().isEmpty()) {
      throw new IAE("Cannot shuffle with spec [%s] and nil clusterBy", shuffleSpec);
    }

    for (final String columnName : signature.getColumnNames()) {
      if (!signature.getColumnType(columnName).isPresent()) {
        throw new ISE("Missing type for column [%s]", columnName);
      }
    }

    for (final int broadcastInputNumber : this.broadcastInputNumbers) {
      if (broadcastInputNumber < 0 || broadcastInputNumber >= inputSpecs.size()) {
        throw new ISE("Broadcast input number out of range [%s]", broadcastInputNumber);
      }
    }
  }

  public static StageDefinitionBuilder builder(final int stageNumber)
  {
    return new StageDefinitionBuilder(stageNumber);
  }

  public static StageDefinitionBuilder builder(final StageDefinition stageDef)
  {
    return new StageDefinitionBuilder(stageDef.getStageNumber())
        .inputs(stageDef.getInputSpecs())
        .broadcastInputs(stageDef.getBroadcastInputNumbers())
        .processorFactory(stageDef.getProcessorFactory())
        .signature(stageDef.getSignature())
        .shuffleSpec(stageDef.doesShuffle() ? stageDef.getShuffleSpec() : null)
        .maxWorkerCount(stageDef.getMaxWorkerCount())
        .shuffleCheckHasMultipleValues(stageDef.getShuffleCheckHasMultipleValues());
  }

  /**
   * Returns a unique stage identifier.
   */
  @JsonProperty
  public StageId getId()
  {
    return id;
  }

  /**
   * Returns input specs for this stage. Positions in this spec list are called "input numbers".
   */
  @JsonProperty("input")
  public List<InputSpec> getInputSpecs()
  {
    return inputSpecs;
  }

  public IntSet getInputStageNumbers()
  {
    return InputSpecs.getStageNumbers(inputSpecs);
  }

  @JsonProperty("broadcast")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public IntSet getBroadcastInputNumbers()
  {
    return broadcastInputNumbers;
  }

  @JsonProperty("processor")
  @SuppressWarnings("rawtypes")
  public FrameProcessorFactory getProcessorFactory()
  {
    return processorFactory;
  }

  @JsonProperty
  public RowSignature getSignature()
  {
    return signature;
  }

  public boolean doesShuffle()
  {
    return shuffleSpec != null;
  }

  public boolean doesSortDuringShuffle()
  {
    if (shuffleSpec == null || shuffleSpec.clusterBy().isEmpty()) {
      return false;
    } else {
      return shuffleSpec.clusterBy().sortable();
    }
  }

  /**
   * Returns the {@link ShuffleSpec} for this stage, if {@link #doesShuffle()}.
   *
   * @throws IllegalStateException if this stage does not shuffle
   */
  public ShuffleSpec getShuffleSpec()
  {
    if (shuffleSpec == null) {
      throw new IllegalStateException("Stage does not shuffle");
    }

    return shuffleSpec;
  }

  /**
   * Returns the {@link ClusterBy} of the {@link ShuffleSpec} if set, otherwise {@link ClusterBy#none()}.
   */
  public ClusterBy getClusterBy()
  {
    if (shuffleSpec != null) {
      return shuffleSpec.clusterBy();
    } else {
      return ClusterBy.none();
    }
  }

  /**
   * Returns the key used for sorting each individual partition, or an empty list if partitions are unsorted.
   */
  public List<KeyColumn> getSortKey()
  {
    final ClusterBy clusterBy = getClusterBy();

    if (clusterBy.sortable()) {
      return clusterBy.getColumns();
    } else {
      return Collections.emptyList();
    }
  }

  @Nullable
  @JsonProperty("shuffleSpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  ShuffleSpec getShuffleSpecForSerialization()
  {
    return shuffleSpec;
  }

  @JsonProperty
  public int getMaxWorkerCount()
  {
    return maxWorkerCount;
  }

  @JsonProperty("shuffleCheckHasMultipleValues")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean getShuffleCheckHasMultipleValues()
  {
    return shuffleCheckHasMultipleValues;
  }

  public int getMaxPartitionCount()
  {
    // Pretends to be an instance method, but really returns a constant. Maybe one day this will be defined per stage.
    return MAX_PARTITIONS;
  }

  public int getStageNumber()
  {
    return id.getStageNumber();
  }

  /**
   * Returns true, if the shuffling stage requires key statistics from the workers.
   * <br></br>
   * Returns false, if the stage does not shuffle.
   * <br></br>
   * <br></br>
   * It's possible we're shuffling using partition boundaries that are known ahead of time
   * For eg: we know there's exactly one partition in query shapes like `select with limit`.
   * <br></br>
   * In such cases, we return a false.
   *
   * @return
   */
  public boolean mustGatherResultKeyStatistics()
  {
    return shuffleSpec != null
           && shuffleSpec.kind() == ShuffleKind.GLOBAL_SORT
           && ((GlobalSortShuffleSpec) shuffleSpec).mustGatherResultKeyStatistics();
  }

  public Either<Long, ClusterByPartitions> generatePartitionBoundariesForShuffle(
      @Nullable ClusterByStatisticsCollector collector
  )
  {
    if (shuffleSpec == null) {
      throw new ISE("No shuffle for stage[%d]", getStageNumber());
    } else if (shuffleSpec.kind() != ShuffleKind.GLOBAL_SORT) {
      throw new ISE(
          "Shuffle of kind [%s] cannot generate partition boundaries for stage[%d]",
          shuffleSpec.kind(),
          getStageNumber()
      );
    } else if (mustGatherResultKeyStatistics() && collector == null) {
      throw new ISE("Statistics required, but not gathered for stage[%d]", getStageNumber());
    } else if (!mustGatherResultKeyStatistics() && collector != null) {
      throw new ISE("Statistics gathered, but not required for stage[%d]", getStageNumber());
    } else {
      return ((GlobalSortShuffleSpec) shuffleSpec).generatePartitionsForGlobalSort(collector, MAX_PARTITIONS);
    }
  }

  public ClusterByStatisticsCollector createResultKeyStatisticsCollector(final int maxRetainedBytes)
  {
    if (!mustGatherResultKeyStatistics()) {
      throw new ISE("No statistics needed for stage[%d]", getStageNumber());
    }

    return ClusterByStatisticsCollectorImpl.create(
        shuffleSpec.clusterBy(),
        signature,
        maxRetainedBytes,
        Limits.MAX_PARTITION_BUCKETS,
        shuffleSpec.doesAggregate(),
        shuffleCheckHasMultipleValues
    );
  }

  /**
   * Create the {@link FrameWriterFactory} that must be used by {@link #getProcessorFactory()}.
   *
   * Calls {@link MemoryAllocatorFactory#newAllocator()} for each frame.
   */
  public FrameWriterFactory createFrameWriterFactory(final MemoryAllocatorFactory memoryAllocatorFactory)
  {
    return FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        memoryAllocatorFactory,
        signature,

        // Main processor does not sort when there is a hash going on, even if isSort = true. This is because
        // FrameChannelHashPartitioner is expected to be attached to the processor and do the sorting. We don't
        // want to double-sort.
        doesShuffle() && !shuffleSpec.kind().isHash() ? getClusterBy().getColumns() : Collections.emptyList()
    );
  }

  /**
   * Create the {@link FrameWriterFactory} that must be used by {@link #getProcessorFactory()}.
   *
   * Re-uses the same {@link MemoryAllocator} for each frame.
   */
  public FrameWriterFactory createFrameWriterFactory(final MemoryAllocator allocator)
  {
    return createFrameWriterFactory(new SingleMemoryAllocatorFactory(allocator));
  }

  public FrameReader getFrameReader()
  {
    return frameReader.get();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StageDefinition that = (StageDefinition) o;
    return maxWorkerCount == that.maxWorkerCount
           && shuffleCheckHasMultipleValues == that.shuffleCheckHasMultipleValues
           && Objects.equals(id, that.id)
           && Objects.equals(inputSpecs, that.inputSpecs)
           && Objects.equals(broadcastInputNumbers, that.broadcastInputNumbers)
           && Objects.equals(processorFactory, that.processorFactory)
           && Objects.equals(signature, that.signature)
           && Objects.equals(shuffleSpec, that.shuffleSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        inputSpecs,
        broadcastInputNumbers,
        processorFactory,
        signature,
        maxWorkerCount,
        shuffleCheckHasMultipleValues,
        shuffleSpec
    );
  }

  @Override
  public String toString()
  {
    return "StageDefinition{" +
           "id=" + id +
           ", inputSpecs=" + inputSpecs +
           (!broadcastInputNumbers.isEmpty() ? ", broadcastInputStages=" + broadcastInputNumbers : "") +
           ", processorFactory=" + processorFactory +
           ", signature=" + signature +
           ", maxWorkerCount=" + maxWorkerCount +
           ", shuffleSpec=" + shuffleSpec +
           (shuffleCheckHasMultipleValues ? ", shuffleCheckHasMultipleValues=" + shuffleCheckHasMultipleValues : "") +
           '}';
  }
}
