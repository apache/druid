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
import org.apache.druid.query.operator.window.value.WindowLagProcessor;
import org.apache.druid.query.operator.window.value.WindowLastProcessor;
import org.apache.druid.query.operator.window.value.WindowLeadProcessor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

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
    @JsonSubTypes.Type(name = "lead", value = WindowLeadProcessor.class),
    @JsonSubTypes.Type(name = "lag", value = WindowLagProcessor.class),
    @JsonSubTypes.Type(name = "aggregate", value = WindowAggregateProcessor.class),
})
public interface Processor
{
   RowsAndColumns process(RowsAndColumns incomingPartition);
}
