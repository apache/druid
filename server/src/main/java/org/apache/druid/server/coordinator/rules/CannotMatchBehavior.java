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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nullable;

/**
 * Controls what happens when a {@link PartialLoadRule}'s {@link PartialLoadMatcher} does not apply to a given segment,
 * for example, a {@link ProjectionPartialLoadMatcher} when faced with a segment that doesn't have projections.
 * <p>
 * Unknown values deserialize to {@code null} so that an older coordinator encountering a rule authored on a newer
 * version that introduces a new behavior falls back to the constructor's default ({@link #FULL_LOAD}) rather than
 * failing to parse the rule.
 */
public enum CannotMatchBehavior
{
  /**
   * The rule does not apply; the cascade continues to the next rule.
   */
  FALL_THROUGH("fallThrough"),

  /**
   * The rule applies and the segment is loaded in full on this tier.
   */
  FULL_LOAD("fullLoad");

  private final String id;

  CannotMatchBehavior(String id)
  {
    this.id = id;
  }

  @JsonCreator
  @Nullable
  public static CannotMatchBehavior fromString(final String id)
  {
    if (id == null) {
      return null;
    }
    for (CannotMatchBehavior behavior : values()) {
      if (behavior.id.equals(id)) {
        return behavior;
      }
    }

    return null;
  }

  @JsonValue
  public String getId()
  {
    return id;
  }

  @Override
  public String toString()
  {
    return id;
  }
}
