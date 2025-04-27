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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.error.InvalidInput;

/**
 * Enum used in the query context to determine if clone queries should be used by native queries.
 */
public enum CloneQueryMode
{
  /**
   * For each ongoing cloning, do not query the source server that is being cloned. Other servers which are not
   * participating in any cloning will still be queried.
   */
  PREFER_CLONES("preferClones"),
  /**
   * Consider both clones and their source servers for querying.
   */
  INCLUDE_CLONES("includeClones"),
  /**
   * Do not query clone servers.
   */
  EXCLUDE_CLONES("excludeClones");

  private final String name;

  CloneQueryMode(String name)
  {
    this.name = name;
  }

  @JsonCreator
  public static CloneQueryMode fromString(String value)
  {
    for (CloneQueryMode mode : values()) {
      if (mode.toString().equals(value)) {
        return mode;
      }
    }

    throw InvalidInput.exception("No such clone query mode[%s]", value);
  }

  @Override
  public String toString()
  {
    return name;
  }
}
