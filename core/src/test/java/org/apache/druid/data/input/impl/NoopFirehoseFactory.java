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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public class NoopFirehoseFactory implements FiniteFirehoseFactory
{
  @Override
  public String toString()
  {
    return "NoopFirehoseFactory{}";
  }

  @Override
  public Stream<InputSplit> getSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FiniteFirehoseFactory withSplit(InputSplit split)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateAllowDenyPrefixList(InputSourceSecurityConfig securityConfig)
  {
    // No URI to validate
  }
}
