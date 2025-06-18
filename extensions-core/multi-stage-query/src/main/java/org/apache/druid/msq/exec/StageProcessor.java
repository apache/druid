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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.msq.exec.std.StandardStageProcessor;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorStageProcessor;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;

/**
 * Encapsulates the computation logic for a {@link StageDefinition}.
 *
 * Each stage has "outputs", which are output channels created by {@link ExecutionContext#outputChannelFactory()}.
 * For stages that shuffle, i.e. where {@link StageDefinition#doesShuffle()}, the outputs must be partitioned according
 * to the {@link StageDefinition#getShuffleSpec()}. For stages that do not shuffle, the output partitioning must
 * align with the input partitioning.
 *
 * Each stage also has a "result", which is an optional, small object produced when all work is complete. Most stages
 * do not have a "result", but some do. For example, the result of {@link SegmentGeneratorStageProcessor} is the set of
 * {@link DataSegment} that have been published.
 *
 * @see StandardStageProcessor for an implementation that handles the shuffle partionining generically
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface StageProcessor<R, ExtraInfoType>
{
  /**
   * Executes work in the executor provided by {@link ExecutionContext#executor()}. Returns immediately. The returned
   * future should resolve when all work is done and all output has been generated.
   *
   * @return stage result future
   */
  ListenableFuture<R> execute(ExecutionContext context);

  /**
   * Whether processors from this factory use {@link ProcessingBuffers}.
   */
  boolean usesProcessingBuffers();

  /**
   * Type reference for the result of this stage.
   */
  @Nullable
  TypeReference<R> getResultTypeReference();

  /**
   * Merges two accumulated results. May modify the left-hand side {@code accumulated}. Does not modify the right-hand
   * side {@code current}.
   */
  R mergeAccumulatedResult(R accumulated, R otherAccumulated);

  /**
   * Produces an {@link ExtraInfoHolder} wrapper that allows serialization of {@code ExtraInfoType}.
   */
  @SuppressWarnings("rawtypes")
  ExtraInfoHolder makeExtraInfoHolder(@Nullable ExtraInfoType extra);
}
