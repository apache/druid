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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Selects partial load of projections whose names appear in the configured {@link #names} list.
 */
public class ExactProjectionPartialLoadMatcher extends ProjectionPartialLoadMatcher
{
  public static final String TYPE = "exactProjection";

  private final List<String> names;

  @JsonCreator
  public ExactProjectionPartialLoadMatcher(@JsonProperty("names") List<String> names)
  {
    if (names == null || names.isEmpty()) {
      throw InvalidInput.exception("names must not be null or empty for exactProjection matcher");
    }
    this.names = List.copyOf(names);
  }

  @JsonProperty
  public List<String> getNames()
  {
    return names;
  }

  @Override
  protected List<String> resolveProjectionNames(DataSegment segment)
  {
    final List<String> segmentProjections = segment.getProjections();
    if (segmentProjections == null || segmentProjections.isEmpty()) {
      return Collections.emptyList();
    }
    final Set<String> present = new HashSet<>(segmentProjections);
    final TreeSet<String> intersected = new TreeSet<>();
    for (String name : names) {
      if (present.contains(name)) {
        intersected.add(name);
      }
    }
    return new ArrayList<>(intersected);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExactProjectionPartialLoadMatcher that = (ExactProjectionPartialLoadMatcher) o;
    return Objects.equals(names, that.names);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(names);
  }

  @Override
  public String toString()
  {
    return "ExactProjectionPartialLoadMatcher{names=" + names + "}";
  }
}
