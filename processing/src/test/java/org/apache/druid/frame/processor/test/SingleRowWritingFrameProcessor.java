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

package org.apache.druid.frame.processor.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;

public class SingleRowWritingFrameProcessor extends SingleChannelFrameProcessor<Long>
{
  private final InputRow inputRow;

  public SingleRowWritingFrameProcessor(WritableFrameChannel writableFrameChannel, InputRow inputRow)
  {
    super(null, writableFrameChannel);
    this.inputRow = inputRow;
  }

  @Override
  public Long doSimpleWork() throws IOException
  {
    final WritableFrameChannel outputChannel = Iterables.getOnlyElement(outputChannels());
    outputChannel.write(TestFrameProcessorUtils.toFrame(ImmutableList.of(inputRow)));
    return 1L;
  }
}
