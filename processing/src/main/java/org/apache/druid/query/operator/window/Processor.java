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

package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.query.operator.window.ranking.WindowCumeDistProcessor;
import org.apache.druid.query.operator.window.ranking.WindowDenseRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowPercentileProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.operator.window.value.WindowFirstProcessor;
import org.apache.druid.query.operator.window.value.WindowLastProcessor;
import org.apache.druid.query.operator.window.value.WindowOffsetProcessor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

/**
 * A Processor is a bit of logic that processes a single RowsAndColumns object to produce a new RowsAndColumns
 * object.  Generally speaking, it is used to add or alter columns in a batch-oriented fashion.
 * <p>
 * This interface was created to support windowing functions, where the windowing function can be implemented
 * assuming that each RowsAndColumns object represents one partition.  Thus, the window function implementation
 * can only need to worry about how to process a single partition at a time and something external to the window
 * function worries about providing data with the correct partitioning.
 * <p>
 * Over time, it's possible that this interface is used for other purposes as well, but the fundamental idea of
 * usages of the interface should always be doing a one-to-one transformation of RowsAndColumns objects.  That is,
 * it's a RowsAndColumns in and a RowsAndColumns out.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "composing", value = ComposingProcessor.class),
    @JsonSubTypes.Type(name = "cumeDist", value = WindowCumeDistProcessor.class),
    @JsonSubTypes.Type(name = "denseRank", value = WindowDenseRankProcessor.class),
    @JsonSubTypes.Type(name = "percentile", value = WindowPercentileProcessor.class),
    @JsonSubTypes.Type(name = "rank", value = WindowRankProcessor.class),
    @JsonSubTypes.Type(name = "rowNumber", value = WindowRowNumberProcessor.class),
    @JsonSubTypes.Type(name = "first", value = WindowFirstProcessor.class),
    @JsonSubTypes.Type(name = "last", value = WindowLastProcessor.class),
    @JsonSubTypes.Type(name = "offset", value = WindowOffsetProcessor.class),
    @JsonSubTypes.Type(name = "framedAgg", value = WindowFramedAggregateProcessor.class)
})
public interface Processor
{
  /**
   * Applies the logic of the processor to a RowsAndColumns object
   *
   * @param incomingPartition the incoming RowsAndColumns object
   * @return the transformed RowsAndColumns object
   */
  RowsAndColumns process(RowsAndColumns incomingPartition);

  /**
   * Validates the equivalence of the Processors.  This is similar to @{code .equals} but is its own method
   * so that it can ignore certain fields that would be important for a true equality check.  Namely, two Processors
   * defined the same way but with different output names can be considered equivalent even though they are not equal.
   * <p>
   * This primarily exists to simplify tests, where this equivalence can be used to validate that the Processors
   * created by the SQL planner are actually equivalent to what we expect without needing to be overly dependent on
   * how the planner names the output columns
   *
   * @param otherProcessor the processor to test equivalence of
   * @return boolean identifying if these processors should be considered equivalent to each other.
   */
  boolean validateEquivalent(Processor otherProcessor);
}
