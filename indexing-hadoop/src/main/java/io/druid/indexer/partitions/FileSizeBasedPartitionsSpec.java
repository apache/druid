/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.Jobby;
import io.druid.indexer.path.StaticPathSpec;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class FileSizeBasedPartitionsSpec extends AbstractPartitionsSpec
{
  @JsonCreator
  public FileSizeBasedPartitionsSpec(
      @JsonProperty("targetPartitionSize") @Nullable Long targetPartitionSize
  )
  {
    super(targetPartitionSize, null, null, null);
  }

  @Override
  public Jobby getPartitionJob(HadoopDruidIndexerConfig config)
  {
    Preconditions.checkArgument(config.getPathSpec() instanceof StaticPathSpec, "Only supports static spec, for now");
    return new DetermineSizeBasedPartitionsJob(config);
  }

  @Override
  public List<String> getPartitionDimensions()
  {
    return ImmutableList.of();
  }
}
