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

package org.apache.druid.msq.querykit.common;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.std.BasicStageProcessor;
import org.apache.druid.msq.exec.std.ProcessorsAndChannels;
import org.apache.druid.msq.exec.std.StandardStageRunner;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Factory for {@link SortMergeJoinFrameProcessor}, which does a sort-merge join of two inputs.
 */
@JsonTypeName("sortMergeJoin")
public class SortMergeJoinStageProcessor extends BasicStageProcessor
{
  private static final int LEFT = 0;
  private static final int RIGHT = 1;

  private final String rightPrefix;
  private final JoinConditionAnalysis condition;
  private final JoinType joinType;

  public SortMergeJoinStageProcessor(
      final String rightPrefix,
      final JoinConditionAnalysis condition,
      final JoinType joinType
  )
  {
    this.rightPrefix = Preconditions.checkNotNull(rightPrefix, "rightPrefix");
    this.condition = validateCondition(Preconditions.checkNotNull(condition, "condition"));
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
  }

  @JsonCreator
  public static SortMergeJoinStageProcessor create(
      @JsonProperty("rightPrefix") String rightPrefix,
      @JsonProperty("condition") String condition,
      @JsonProperty("joinType") JoinType joinType,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    return new SortMergeJoinStageProcessor(
        StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
        JoinConditionAnalysis.forExpression(
            Preconditions.checkNotNull(condition, "condition"),
            StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
            macroTable
        ),
        joinType
    );
  }

  @JsonProperty
  public String getRightPrefix()
  {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition()
  {
    return condition.getOriginalExpression();
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @Override
  public ListenableFuture<Long> execute(ExecutionContext context)
  {
    final StandardStageRunner<Object, Long> stageRunner = new StandardStageRunner<>(context);

    final List<InputSlice> inputSlices = context.workOrder().getInputs();
    if (inputSlices.size() != 2 || !inputSlices.stream().allMatch(slice -> slice instanceof StageInputSlice)) {
      // Can't hit this unless there was some bug in QueryKit.
      throw new ISE("Expected two stage inputs");
    }

    // Compute key columns.
    final List<List<KeyColumn>> keyColumns = toKeyColumns(condition);
    final int[] requiredNonNullKeyParts = toRequiredNonNullKeyParts(condition);

    // Stitch up the inputs and validate each input channel signature.
    // If validateInputFrameSignatures fails, it's a precondition violation: this class somehow got bad inputs.
    final Int2ObjectMap<List<ReadableInput>> inputsByPartition = validateInputFrameSignatures(
        collectAndReadPartitions(context),
        keyColumns
    );

    if (inputsByPartition.isEmpty()) {
      return stageRunner.run(
          new ProcessorsAndChannels<>(ProcessorManagers.none(), OutputChannels.none())
      );
    }

    // Create output channels.
    final Int2ObjectMap<OutputChannel> outputChannels = new Int2ObjectAVLTreeMap<>();
    for (int partitionNumber : inputsByPartition.keySet()) {
      try {
        outputChannels.put(partitionNumber, stageRunner.workOutputChannelFactory().openChannel(partitionNumber));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Create processors.
    final Iterable<FrameProcessor<Object>> processors = Iterables.transform(
        inputsByPartition.int2ObjectEntrySet(),
        entry -> {
          final int partitionNumber = entry.getIntKey();
          final List<ReadableInput> readableInputs = entry.getValue();
          final OutputChannel outputChannel = outputChannels.get(partitionNumber);

          return new SortMergeJoinFrameProcessor(
              readableInputs.get(LEFT),
              readableInputs.get(RIGHT),
              outputChannel.getWritableChannel(),
              context.workOrder().getStageDefinition().createFrameWriterFactory(
                  context.frameContext().frameWriterSpec(),
                  outputChannel.getFrameMemoryAllocator()
              ),
              rightPrefix,
              keyColumns,
              requiredNonNullKeyParts,
              joinType,
              context.frameContext().memoryParameters().getSortMergeJoinMemory()
          );
        }
    );

    return stageRunner.run(
        new ProcessorsAndChannels<>(
            ProcessorManagers.of(processors),
            OutputChannels.wrap(ImmutableList.copyOf(outputChannels.values()))
        )
    );
  }

  @Override
  public boolean usesProcessingBuffers()
  {
    return false;
  }

  /**
   * Extracts key columns from a {@link JoinConditionAnalysis}. The returned list has two elements: 0 is the
   * left-hand side, 1 is the right-hand side. Each sub-list has one element for each equi-condition.
   *
   * The condition must have been validated by {@link #validateCondition(JoinConditionAnalysis)}.
   */
  public static List<List<KeyColumn>> toKeyColumns(final JoinConditionAnalysis condition)
  {
    final List<List<KeyColumn>> retVal = new ArrayList<>();
    retVal.add(new ArrayList<>()); // Left-side key columns
    retVal.add(new ArrayList<>()); // Right-side key columns

    for (final Equality equiCondition : condition.getEquiConditions()) {
      final String leftColumn = Preconditions.checkNotNull(
          equiCondition.getLeftExpr().getBindingIfIdentifier(),
          "leftExpr#getBindingIfIdentifier"
      );

      retVal.get(0).add(new KeyColumn(leftColumn, KeyOrder.ASCENDING));
      retVal.get(1).add(new KeyColumn(equiCondition.getRightColumn(), KeyOrder.ASCENDING));
    }

    return retVal;
  }

  /**
   * Extracts a list of key parts that must be nonnull from a {@link JoinConditionAnalysis}. These are equality
   * conditions for which {@link Equality#isIncludeNull()} is false.
   *
   * The condition must have been validated by {@link #validateCondition(JoinConditionAnalysis)}.
   */
  public static int[] toRequiredNonNullKeyParts(final JoinConditionAnalysis condition)
  {
    final IntList retVal = new IntArrayList(condition.getEquiConditions().size());

    final List<Equality> equiConditions = condition.getEquiConditions();
    for (int i = 0; i < equiConditions.size(); i++) {
      Equality equiCondition = equiConditions.get(i);
      if (!equiCondition.isIncludeNull()) {
        retVal.add(i);
      }
    }

    return retVal.toArray(new int[0]);
  }

  /**
   * Validates that a join condition can be handled by this processor. Returns the condition if it can be handled.
   * Throws {@link IllegalArgumentException} if the condition cannot be handled.
   */
  public static JoinConditionAnalysis validateCondition(final JoinConditionAnalysis condition)
  {
    if (condition.isAlwaysTrue()) {
      return condition;
    }

    if (condition.isAlwaysFalse()) {
      throw new IAE("Cannot handle constant condition: %s", condition.getOriginalExpression());
    }

    if (condition.getNonEquiConditions().size() > 0) {
      throw new IAE("Cannot handle non-equijoin condition: %s", condition.getOriginalExpression());
    }

    if (condition.getEquiConditions().stream().anyMatch(c -> !c.getLeftExpr().isIdentifier())) {
      throw new IAE(
          "Cannot handle equality condition involving left-hand expression: %s",
          condition.getOriginalExpression()
      );
    }

    return condition;
  }

  /**
   * Validates that all signatures from {@link #collectAndReadPartitions(ExecutionContext)} are prefixed by the
   * provided {@code keyColumns}.
   */
  private static Int2ObjectMap<List<ReadableInput>> validateInputFrameSignatures(
      final Int2ObjectMap<List<ReadableInput>> inputsByPartition,
      final List<List<KeyColumn>> keyColumns
  )
  {
    for (List<ReadableInput> readableInputs : inputsByPartition.values()) {
      for (int i = 0; i < readableInputs.size(); i++) {
        final ReadableInput readableInput = readableInputs.get(i);
        Preconditions.checkState(readableInput.hasChannel(), "readableInput[%s].hasChannel", i);

        final RowSignature signature = readableInput.getChannelFrameReader().signature();
        for (int j = 0; j < keyColumns.get(i).size(); j++) {
          final String columnName = keyColumns.get(i).get(j).columnName();
          Preconditions.checkState(
              columnName.equals(signature.getColumnName(j)),
              "readableInput[%s] column[%s] has expected name[%s]",
              i,
              j,
              columnName
          );
        }
      }
    }

    return inputsByPartition;
  }

  /**
   * Calls {@link InputSliceReader#attach} on all input slices, which must all be {@link NilInputSlice} or
   * {@link StageInputSlice}, and collects like-numbered partitions.
   *
   * The returned map is keyed by partition number. Each value is a list of inputs of the
   * same length as "slices", and in the same order. i.e., the first ReadableInput in each list corresponds to the
   * first provided {@link InputSlice}.
   *
   * "Missing" partitions -- which occur when one slice has no data for a given partition -- are replaced with
   * {@link ReadableInput} based on {@link ReadableNilFrameChannel}, with no {@link StagePartition}.
   *
   * @throws IllegalStateException if any slices are not {@link StageInputSlice} or {@link NilInputSlice}
   */
  private static Int2ObjectMap<List<ReadableInput>> collectAndReadPartitions(final ExecutionContext context)
  {
    final List<InputSlice> slices = context.workOrder().getInputs();

    // Input number -> FrameReader.
    final List<FrameReader> frameReadersByInputNumber = Arrays.asList(new FrameReader[slices.size()]);

    // Partition number -> Input number -> Input channel
    final Int2ObjectMap<List<ReadableInput>> retVal = new Int2ObjectRBTreeMap<>();

    for (int inputNumber = 0; inputNumber < slices.size(); inputNumber++) {
      final InputSlice slice = slices.get(inputNumber);

      if (slice instanceof StageInputSlice) {
        if (frameReadersByInputNumber.get(inputNumber) == null) {
          frameReadersByInputNumber.set(
              inputNumber,
              context.workOrder()
                     .getQueryDefinition()
                     .getStageDefinition(((StageInputSlice) slice).getStageNumber())
                     .getFrameReader()
          );
        }

        final ReadablePartitions partitions = ((StageInputSlice) slice).getPartitions();
        for (final ReadablePartition partition : partitions) {
          retVal.computeIfAbsent(partition.getPartitionNumber(), ignored -> Arrays.asList(new ReadableInput[slices.size()]))
                .set(inputNumber, QueryKitUtils.readPartition(context, partition));
        }
      } else if (!(slice instanceof NilInputSlice)) {
        throw DruidException.defensive("Slice[%s] is not a 'stage' or 'nil' slice", slice);
      }
    }

    // Fill in all nulls with NilInputSlice.
    for (Int2ObjectMap.Entry<List<ReadableInput>> entry : retVal.int2ObjectEntrySet()) {
      for (int inputNumber = 0; inputNumber < entry.getValue().size(); inputNumber++) {
        if (entry.getValue().get(inputNumber) == null) {
          entry.getValue().set(
              inputNumber,
              ReadableInput.channel(
                  ReadableNilFrameChannel.INSTANCE,
                  frameReadersByInputNumber.get(inputNumber),
                  null
              )
          );
        }
      }
    }

    return retVal;
  }
}
