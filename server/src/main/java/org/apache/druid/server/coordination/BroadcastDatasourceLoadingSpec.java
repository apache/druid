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

package org.apache.druid.server.coordination;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class defines the spec for loading of broadcast datasources for a given task. It contains 2 fields:
 * <ol>
 *   <li>{@link BroadcastDatasourceLoadingSpec#mode}: This mode defines whether broadcastDatasources need to be
 *   loaded for the given task, or not. It can take 3 values: </li>
 *   <ul>
 *    <li> ALL: Load all the broadcast datasources.</li>
 *    <li> NONE: Load no broadcast datasources. </li>
 *    <li> ONLY_REQUIRED: Load only the broadcast datasources defined in broadcastDatasourcesToLoad </li>
 *   </ul>
 * <li>{@link BroadcastDatasourceLoadingSpec#broadcastDatasourcesToLoad}: Defines the broadcastDatasources to load when the broadcastDatasourceLoadingMode is set to ONLY_REQUIRED.</li>
 * </ol>
 */
public class BroadcastDatasourceLoadingSpec
{

  public static final String CTX_BROADCAST_DATASOURCE_LOADING_MODE = "broadcastDatasourceLoadingMode";
  public static final String CTX_BROADCAST_DATASOURCES_TO_LOAD = "broadcastDatasourcesToLoad";

  public enum Mode
  {
    ALL, NONE, ONLY_REQUIRED
  }

  private final Mode mode;
  @Nullable
  private final ImmutableSet<String> broadcastDatasourcesToLoad;

  public static final BroadcastDatasourceLoadingSpec ALL = new BroadcastDatasourceLoadingSpec(Mode.ALL, null);
  public static final BroadcastDatasourceLoadingSpec NONE = new BroadcastDatasourceLoadingSpec(Mode.NONE, null);

  private BroadcastDatasourceLoadingSpec(Mode mode, @Nullable Set<String> broadcastDatasourcesToLoad)
  {
    this.mode = mode;
    this.broadcastDatasourcesToLoad = broadcastDatasourcesToLoad == null ? null : ImmutableSet.copyOf(broadcastDatasourcesToLoad);
  }

  /**
   * Creates a BroadcastSegmentLoadingSpec which loads only the broadcast datasources present in the given set.
   */
  public static BroadcastDatasourceLoadingSpec loadOnly(Set<String> broadcastDatasourcesToLoad)
  {
    if (broadcastDatasourcesToLoad == null) {
      throw InvalidInput.exception("Expected non-null set of broadcast datasources to load.");
    }
    return new BroadcastDatasourceLoadingSpec(Mode.ONLY_REQUIRED, broadcastDatasourcesToLoad);
  }

  public Mode getMode()
  {
    return mode;
  }

  /**
   * @return A non-null immutable set of broadcast datasource names when {@link BroadcastDatasourceLoadingSpec#mode} is ONLY_REQUIRED, null otherwise.
   */
  public ImmutableSet<String> getBroadcastDatasourcesToLoad()
  {
    return broadcastDatasourcesToLoad;
  }

  public static BroadcastDatasourceLoadingSpec createFromContext(Map<String, Object> context, BroadcastDatasourceLoadingSpec defaultSpec)
  {
    if (context == null) {
      return defaultSpec;
    }

    final Object broadcastDatasourceModeValue = context.get(CTX_BROADCAST_DATASOURCE_LOADING_MODE);
    if (broadcastDatasourceModeValue == null) {
      return defaultSpec;
    }

    final BroadcastDatasourceLoadingSpec.Mode broadcastDatasourceLoadingMode;
    try {
      broadcastDatasourceLoadingMode = BroadcastDatasourceLoadingSpec.Mode.valueOf(broadcastDatasourceModeValue.toString());
    }
    catch (IllegalArgumentException e) {
      throw InvalidInput.exception(
          "Invalid value of %s[%s]. Allowed values are %s",
          CTX_BROADCAST_DATASOURCE_LOADING_MODE, broadcastDatasourceModeValue.toString(),
          Arrays.asList(BroadcastDatasourceLoadingSpec.Mode.values())
      );
    }

    if (broadcastDatasourceLoadingMode == Mode.NONE) {
      return NONE;
    } else if (broadcastDatasourceLoadingMode == Mode.ALL) {
      return ALL;
    } else if (broadcastDatasourceLoadingMode == Mode.ONLY_REQUIRED) {
      final Collection<String> broadcastDatasourcesToLoad;
      try {
        broadcastDatasourcesToLoad = (Collection<String>) context.get(CTX_BROADCAST_DATASOURCES_TO_LOAD);
      }
      catch (ClassCastException e) {
        throw InvalidInput.exception(
            "Invalid value of %s[%s]. Please provide a comma-separated list of broadcast datasource names."
            + " For example: [\"datasourceName1\", \"datasourceName2\"]",
            CTX_BROADCAST_DATASOURCES_TO_LOAD, context.get(CTX_BROADCAST_DATASOURCES_TO_LOAD)
        );
      }

      if (broadcastDatasourcesToLoad == null || broadcastDatasourcesToLoad.isEmpty()) {
        throw InvalidInput.exception("Set of broadcast datasources to load cannot be %s for mode[ONLY_REQUIRED].", broadcastDatasourcesToLoad);
      }
      return BroadcastDatasourceLoadingSpec.loadOnly(new HashSet<>(broadcastDatasourcesToLoad));
    } else {
      return defaultSpec;
    }
  }

  @Override
  public String toString()
  {
    return "BroadcastDatasourceLoadingSpec{" +
           "mode=" + mode +
           ", broadcastDatasourcesToLoad=" + broadcastDatasourcesToLoad +
           '}';
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
    BroadcastDatasourceLoadingSpec that = (BroadcastDatasourceLoadingSpec) o;
    return mode == that.mode && Objects.equals(broadcastDatasourcesToLoad, that.broadcastDatasourcesToLoad);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(mode, broadcastDatasourcesToLoad);
  }
}
