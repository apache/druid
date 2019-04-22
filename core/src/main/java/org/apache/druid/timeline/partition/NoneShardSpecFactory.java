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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

public class NoneShardSpecFactory implements ShardSpecFactory
{
  private static NoneShardSpecFactory INSTANCE = new NoneShardSpecFactory();

  public static NoneShardSpecFactory instance()
  {
    return INSTANCE;
  }

  private NoneShardSpecFactory()
  {
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    return NoneShardSpec.instance();
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, int partitionId)
  {
    return NoneShardSpec.instance();
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return NoneShardSpec.class;
  }
}
