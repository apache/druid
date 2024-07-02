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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * Encapsulates the Engine to be used for a compaction task.
 * Should be kept in sync with the subtypes for {@link org.apache.druid.indexing.common.task.CompactionRunner}.
 */
public enum CompactionEngine
{
  NATIVE,
  MSQ;

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(this.name());
  }

  @JsonCreator
  public static CompactionEngine fromString(@Nullable String name)
  {
    return name == null ? null : valueOf(StringUtils.toUpperCase(name));
  }
}
