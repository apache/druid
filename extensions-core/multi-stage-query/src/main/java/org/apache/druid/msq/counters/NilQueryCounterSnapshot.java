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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Represents an unknown counter type. This is the "defaultType" for {@link QueryCounterSnapshot}, so it is
 * substituted at deserialization time if the type is unknown. This can happen when running mixed versions, where some
 * servers support a newer counter type and some don't.
 */
@JsonTypeName("nil")
public class NilQueryCounterSnapshot implements QueryCounterSnapshot
{
  private NilQueryCounterSnapshot()
  {
    // Singleton
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }
}
