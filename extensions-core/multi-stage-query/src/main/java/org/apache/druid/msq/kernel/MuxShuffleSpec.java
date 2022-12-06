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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;

public class MuxShuffleSpec implements ShuffleSpec
{
  public static final String TYPE = "mux";

  private static final MuxShuffleSpec INSTANCE = new MuxShuffleSpec();

  private MuxShuffleSpec()
  {
  }

  @JsonCreator
  public static MuxShuffleSpec instance()
  {
    return INSTANCE;
  }

  @Override
  public ShuffleKind kind()
  {
    return ShuffleKind.MUX;
  }

  @Override
  public ClusterBy clusterBy()
  {
    return ClusterBy.none();
  }

  @Override
  public boolean doesAggregate()
  {
    return false;
  }

  @Override
  public boolean needsStatistics()
  {
    return false;
  }

  @Override
  public int partitionCount()
  {
    return 1;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitionsForGlobalSort(
      @Nullable ClusterByStatisticsCollector collector,
      int maxNumPartitions
  )
  {
    throw new IllegalStateException("Not a global sort");
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj != null && this.getClass().equals(obj.getClass());
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "MuxShuffleSpec{}";
  }
}
