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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

import java.util.Arrays;

public enum JoinAlgorithm
{
  BROADCAST("broadcast") {
    @Override
    public boolean requiresSubquery()
    {
      return false;
    }
  },
  SORT_MERGE("sortMerge") {
    @Override
    public boolean requiresSubquery()
    {
      return true;
    }
  };

  private final String id;

  JoinAlgorithm(String id)
  {
    this.id = id;
  }

  @JsonCreator
  public static JoinAlgorithm fromString(final String id)
  {
    for (final JoinAlgorithm value : values()) {
      if (value.id.equals(id)) {
        return value;
      }
    }

    throw new IAE("No such join algorithm [%s]. Supported values are: %s", id, Arrays.toString(values()));
  }

  @JsonValue
  public String getId()
  {
    return id;
  }

  /**
   * Whether this join algorithm requires subqueries for all inputs.
   */
  public abstract boolean requiresSubquery();

  @Override
  public String toString()
  {
    return id;
  }
}
