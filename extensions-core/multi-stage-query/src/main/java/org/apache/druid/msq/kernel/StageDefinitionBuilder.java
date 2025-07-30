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

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builder for {@link StageDefinition}. See class-level javadoc for that class for a description of the parameters.
 */
public class StageDefinitionBuilder
{
  private final int stageNumber;
  private final List<InputSpec> inputSpecs = new ArrayList<>();
  private final IntSet broadcastInputNumbers = new IntRBTreeSet();
  @SuppressWarnings("rawtypes")
  private StageProcessor processor;
  private RowSignature signature = RowSignature.empty();
  private int maxWorkerCount = 1;
  private ShuffleSpec shuffleSpec = null;
  private boolean shuffleCheckHasMultipleValues = false;

  /**
   * Package-private: callers should prefer {@link StageDefinition#builder(int)} rather than this constructor.
   */
  StageDefinitionBuilder(final int stageNumber)
  {
    this.stageNumber = stageNumber;
  }

  StageDefinitionBuilder(StageDefinitionBuilder other)
  {
    this.stageNumber = other.stageNumber;
    inputs(other.inputSpecs);
    broadcastInputs(other.broadcastInputNumbers);
    processor(other.processor);
    signature(other.signature);
    maxWorkerCount(other.maxWorkerCount);
    shuffleSpec(other.shuffleSpec);
    shuffleCheckHasMultipleValues(other.shuffleCheckHasMultipleValues);
  }

  public StageDefinitionBuilder inputs(final List<InputSpec> inputSpecs)
  {
    this.inputSpecs.clear();
    this.inputSpecs.addAll(inputSpecs);
    return this;
  }

  public StageDefinitionBuilder inputs(final InputSpec... inputSpecs)
  {
    return inputs(Arrays.asList(inputSpecs));
  }

  public StageDefinitionBuilder broadcastInputs(final IntSet broadcastInputNumbers)
  {
    this.broadcastInputNumbers.clear();

    for (int broadcastInputNumber : broadcastInputNumbers) {
      this.broadcastInputNumbers.add(broadcastInputNumber);
    }

    return this;
  }

  @SuppressWarnings("rawtypes")
  public StageDefinitionBuilder processor(final StageProcessor processor)
  {
    this.processor = processor;
    return this;
  }

  public StageDefinitionBuilder signature(final RowSignature signature)
  {
    this.signature = signature;
    return this;
  }

  public StageDefinitionBuilder maxWorkerCount(final int maxWorkerCount)
  {
    this.maxWorkerCount = maxWorkerCount;
    return this;
  }

  public StageDefinitionBuilder shuffleCheckHasMultipleValues(final boolean shuffleCheckHasMultipleValues)
  {
    this.shuffleCheckHasMultipleValues = shuffleCheckHasMultipleValues;
    return this;
  }

  public StageDefinitionBuilder shuffleSpec(final ShuffleSpec shuffleSpec)
  {
    this.shuffleSpec = shuffleSpec;
    return this;
  }

  public int getStageNumber()
  {
    return stageNumber;
  }

  public RowSignature getSignature()
  {
    return signature;
  }

  public StageDefinition build(final String queryId)
  {
    return new StageDefinition(
        new StageId(queryId, stageNumber),
        inputSpecs,
        broadcastInputNumbers,
        processor,
        signature,
        shuffleSpec,
        maxWorkerCount,
        shuffleCheckHasMultipleValues
    );
  }

  public StageDefinitionBuilder copy()
  {
    return new StageDefinitionBuilder(this);
  }
}
