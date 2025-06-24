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
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.std.BasicStandardStageProcessor;
import org.apache.druid.msq.exec.std.ProcessorsAndChannels;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Factory for {@link SortMergeJoinFrameProcessor}, which does a sort-merge join of two inputs.
 */
@JsonTypeName("sortMergeJoin")
public class SortMergeJoinStageProcessor extends BasicStandardStageProcessor
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
  public ProcessorsAndChannels<Object, Long> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher,
      boolean removeNullBytes
  ) throws IOException
  {
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
        InputSlices.attachAndCollectPartitions(
            inputSlices,
            inputSliceReader,
            counters,
            warningPublisher
        ),
        keyColumns
    );

    if (inputsByPartition.isEmpty()) {
      return new ProcessorsAndChannels<>(ProcessorManagers.none(), OutputChannels.none());
    }

    // Create output channels.
    final Int2ObjectMap<OutputChannel> outputChannels = new Int2ObjectAVLTreeMap<>();
    for (int partitionNumber : inputsByPartition.keySet()) {
      outputChannels.put(partitionNumber, outputChannelFactory.openChannel(partitionNumber));
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
              stageDefinition.createFrameWriterFactory(outputChannel.getFrameMemoryAllocator(), removeNullBytes),
              rightPrefix,
              keyColumns,
              requiredNonNullKeyParts,
              joinType,
              frameContext.memoryParameters().getSortMergeJoinMemory()
          );
        }
    );

    return new ProcessorsAndChannels<>(
        ProcessorManagers.of(processors),
        OutputChannels.wrap(ImmutableList.copyOf(outputChannels.values()))
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
   * Validates that all signatures from {@link InputSlices#attachAndCollectPartitions} are prefixed by the
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
}
