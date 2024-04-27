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

package org.apache.druid.server.lookup.cache;

import org.apache.druid.java.util.common.ISE;

import java.util.List;

/**
 * This class defines the spec for loading of lookups for a given task. It contains 2 fields:
 * <ol>
 *   <li>{@link LookupLoadingSpec#mode}: This mode defines whether lookups need to be
 *   loaded for the given task, or not. It can take 3 values: </li>
 *   <ul>
 *    <li> ALL: Load all the lookups.</li>
 *    <li> NONE: Load no lookups. </li>
 *    <li> PARTIAL: Load only the lookups defined in lookupsToLoad </li>
 *   </ul>
 * <li>{@link LookupLoadingSpec#lookupsToLoad}: Defines the lookups to load when the lookupLoadingMode is set to PARTIAL.</li>
 * </ol>
 */
public class LookupLoadingSpec
{
  public enum Mode
  {
    ALL, NONE, PARTIAL
  }

  private final Mode mode;
  private final List<String> lookupsToLoad;

  public static final LookupLoadingSpec ALL = new LookupLoadingSpec(Mode.ALL, null);
  public static final LookupLoadingSpec NONE = new LookupLoadingSpec(Mode.NONE, null);

  private LookupLoadingSpec(Mode mode, List<String> lookupsToLoad)
  {
    this.mode = mode;
    this.lookupsToLoad = lookupsToLoad;
  }

  public static LookupLoadingSpec partial(List<String> lookupsToLoad)
  {
    if (lookupsToLoad == null) {
      throw new ISE("Expected non-null list of lookups to load.");
    }
    return new LookupLoadingSpec(Mode.PARTIAL, lookupsToLoad);
  }

  public Mode getMode()
  {
    return mode;
  }

  public List<String> getLookupsToLoad()
  {
    return lookupsToLoad;
  }
}
