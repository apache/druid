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
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.operator.OperatorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@JsonTypeName("leafWindow")
public class WindowLeafOperatorProcessorFactory extends BaseFrameProcessorFactory
{
  private final List<OperatorFactory> operatorFactoryList;

  @JsonCreator
  public WindowLeafOperatorProcessorFactory(List<OperatorFactory> operatorFactoryList)
  {
    this.operatorFactoryList = operatorFactoryList;
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
      Consumer<Throwable> warningPublisher
  ) throws IOException
  {
    return null;
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
    WindowLeafOperatorProcessorFactory that = (WindowLeafOperatorProcessorFactory) o;
    return Objects.equals(operatorFactoryList, that.operatorFactoryList);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(operatorFactoryList);
  }
}
