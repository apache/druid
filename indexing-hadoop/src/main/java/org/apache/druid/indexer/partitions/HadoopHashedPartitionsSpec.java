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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.DetermineHashedPartitionsJob;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.Jobby;

import javax.annotation.Nullable;
import java.util.List;

public class HadoopHashedPartitionsSpec extends HashedPartitionsSpec implements HadoopPartitionsSpec
{
  public static HadoopHashedPartitionsSpec defaultSpec()
  {
    return new HadoopHashedPartitionsSpec(null, null, null);
  }

  @JsonCreator
  public HadoopHashedPartitionsSpec(
      @JsonProperty("targetPartitionSize") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions
  )
  {
    super(maxRowsPerSegment, numShards, partitionDimensions);
  }

  @Nullable
  @Override
  @JsonProperty("targetPartitionSize")
  public Integer getMaxRowsPerSegment()
  {
    return super.getMaxRowsPerSegment();
  }

  @Override
  public Jobby getPartitionJob(HadoopDruidIndexerConfig config)
  {
    return new DetermineHashedPartitionsJob(config);
  }

  @Override
  public boolean needsDeterminePartitions()
  {
    return getMaxRowsPerSegment() != null;
  }

  @Override
  public String toString()
  {
    return "HadoopHashedPartitionsSpec{} " + super.toString();
  }
}
