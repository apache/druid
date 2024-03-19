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

/**
 * Shuffle spec that generates a single, unsorted partition.
 */
public class MixShuffleSpec implements ShuffleSpec
{
  public static final String TYPE = "mix";

  private static final MixShuffleSpec INSTANCE = new MixShuffleSpec();

  private MixShuffleSpec()
  {
  }

  @JsonCreator
  public static MixShuffleSpec instance()
  {
    return INSTANCE;
  }

  @Override
  public ShuffleKind kind()
  {
    return ShuffleKind.MIX;
  }

  @Override
  public ClusterBy clusterBy()
  {
    return ClusterBy.none();
  }

  @Override
  public int partitionCount()
  {
    return 1;
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
    return "MixShuffleSpec{}";
  }
}
