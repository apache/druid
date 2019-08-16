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

public class NumberedShardSpecFactory implements ShardSpecFactory
{
  private static final NumberedShardSpecFactory INSTANCE = new NumberedShardSpecFactory();

  public static NumberedShardSpecFactory instance()
  {
    return INSTANCE;
  }

  private NumberedShardSpecFactory()
  {
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    if (specOfPreviousMaxPartitionId == null) {
      return new NumberedShardSpec(0, 0);
    } else {
      final NumberedShardSpec prevSpec = (NumberedShardSpec) specOfPreviousMaxPartitionId;
      return new NumberedShardSpec(prevSpec.getPartitionNum() + 1, prevSpec.getPartitions());
    }
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, int partitionId)
  {
    return new NumberedShardSpec(partitionId, 0);
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return NumberedShardSpec.class;
  }
}
