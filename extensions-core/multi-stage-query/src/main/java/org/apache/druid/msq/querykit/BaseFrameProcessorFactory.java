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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.kernel.ExtraInfoHolder;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.NilExtraInfoHolder;

import javax.annotation.Nullable;

/**
 * Basic abstract {@link FrameProcessorFactory} that yields workers that do not require extra info and that
 * always return Longs. This base class isn't used for every worker factory, but it is used for many of them.
 */
public abstract class BaseFrameProcessorFactory
    implements FrameProcessorFactory<Object, FrameProcessor<Long>, Long, Long>
{
  @Override
  public TypeReference<Long> getAccumulatedResultTypeReference()
  {
    return new TypeReference<Long>() {};
  }

  @Override
  public Long newAccumulatedResult()
  {
    return 0L;
  }

  @Nullable
  @Override
  public Long accumulateResult(Long accumulated, Long current)
  {
    return accumulated + current;
  }

  @Override
  public Long mergeAccumulatedResult(Long accumulated, Long otherAccumulated)
  {
    return accumulated + otherAccumulated;
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    if (extra != null) {
      throw new ISE("Expected null 'extra'");
    }

    return NilExtraInfoHolder.instance();
  }
}
