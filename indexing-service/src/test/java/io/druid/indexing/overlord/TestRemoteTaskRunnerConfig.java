/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.joda.time.Period;

/**
 */
public class TestRemoteTaskRunnerConfig extends RemoteTaskRunnerConfig
{
  @Override
  public Period getTaskAssignmentTimeout()
  {
    return new Period("PT1S");
  }

  @Override
  public long getMaxZnodeBytes()
  {
    // make sure this is large enough, otherwise RemoteTaskRunnerTest might fail unexpectedly
    return 10 * 1024;
  }

  @Override
  public String getMinWorkerVersion()
  {
    return "";
  }
}
