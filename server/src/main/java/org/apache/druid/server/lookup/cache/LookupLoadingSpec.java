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

import java.util.List;

public class LookupLoadingSpec
{
  /**
   * This class defines the spec for loading of lookups for a given task. It contains 2 fields:
   * <ol>
   *   <li>{@link LookupLoadingSpec#lookupLoadingMode}: This mode defines whether lookups need to be
   *   loaded for the given task, or not. It can take 3 values: </li>
   *   <ul>
   *    <li> ALL: Load all the lookups.</li>
   *    <li> NONE: Load no lookups. </li>
   *    <li> PARTIAL: Load only the lookups defined in lookupsToLoad </li>
   *   </ul>
   * <li>{@link LookupLoadingSpec#lookupsToLoad}: Defines the lookups to load when the lookupLoadingMode is set to PARTIAL.</li>
   * </ol>
   */
  public enum LookupLoadingMode
  {
    ALL, NONE, PARTIAL
  }

  public LookupLoadingMode lookupLoadingMode;
  public List<String> lookupsToLoad;

  public LookupLoadingSpec(LookupLoadingMode lookupLoadingMode, List<String> lookupsToLoad)
  {
    this.lookupLoadingMode = lookupLoadingMode;
    this.lookupsToLoad = lookupsToLoad;
  }

  public LookupLoadingSpec()
  {
    this.lookupLoadingMode = LookupLoadingMode.ALL;
  }

  public LookupLoadingMode getLookupLoadingMode()
  {
    return lookupLoadingMode;
  }

  public List<String> getLookupsToLoad()
  {
    return lookupsToLoad;
  }
}
