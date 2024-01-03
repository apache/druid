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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.function.Function;

@JsonTypeName("window")
public class WindowOperatorQueryFrameProcessorFactory extends BaseLeafFrameProcessorFactory
{
  private final WindowOperatorQuery query;
  private final List<OperatorFactory> operatorList;
  RowSignature rearrangePoiter;

  @JsonCreator
  public WindowOperatorQueryFrameProcessorFactory(
      @JsonProperty("query") WindowOperatorQuery query,
      @JsonProperty("operators") List<OperatorFactory> operatorFactoryList,
      RowSignature pointers
  )
  {
    super(query);
    this.query = Preconditions.checkNotNull(query, "query");
    this.operatorList = operatorFactoryList;
    this.rearrangePoiter = pointers;
  }

  @JsonProperty
  public WindowOperatorQuery getQuery()
  {
    return query;
  }

  @JsonProperty
  public List<OperatorFactory> getOperators()
  {
    return operatorList;
  }

  @Override
  protected FrameProcessor<Object> makeProcessor(
      ReadableInput baseInput,
      Function<SegmentReference, SegmentReference> segmentMapFn,
      ResourceHolder<WritableFrameChannel> outputChannelHolder,
      ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      FrameContext frameContext
  )
  {
    return new WindowOperatorQueryFrameProcessor(
        query,
        operatorList,
        frameContext.jsonMapper(),
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder,
        rearrangePoiter
    );
  }
}
