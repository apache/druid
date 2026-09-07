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

package org.apache.druid.java.util.metrics;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A TaskHolder implementation for all servers that are not {@code CliPeon}.
 *
 * <p>This holder does not provide task information and will return null from
 * all its methods.</p>
 */
public class NoopTaskHolder implements TaskHolder
{
  @Nullable
  @Override
  public String getDataSource()
  {
    return null;
  }

  @Nullable
  @Override
  public String getTaskId()
  {
    return null;
  }

  @Nullable
  @Override
  public String getTaskType()
  {
    return null;
  }

  @Nullable
  @Override
  public String getGroupId()
  {
    return null;
  }

  @Override
  public Map<String, String> getMetricDimensions()
  {
    return Map.of();
  }
}
