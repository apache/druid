/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

public class LookupConfig
{

  @JsonProperty
  private final String snapshotWorkingDir;

  @JsonProperty
  private int numLookupLoadingThreads = 10;

  @JsonProperty
  private boolean enableLookupSyncOnStartup;

  /**
   * @param snapshotWorkingDir working directory to store lookups snapshot file, passing null or empty string will disable the snapshot utility
   * @param numLookupLoadingThreads number of threads for loading the lookups as part of the synchronization process
   * @param enableLookupSyncOnStartup decides whether the lookup synchronization process should be enabled at startup
   */
  @JsonCreator
  public LookupConfig(
      @JsonProperty("snapshotWorkingDir") String snapshotWorkingDir,
      @JsonProperty("numLookupLoadingThreads") int numLookupLoadingThreads,
      @JsonProperty("enableLookupSyncOnStartup") Boolean enableLookupSyncOnStartup
  )
  {
    this.snapshotWorkingDir = Strings.nullToEmpty(snapshotWorkingDir);
    this.numLookupLoadingThreads = numLookupLoadingThreads;
    this.enableLookupSyncOnStartup = enableLookupSyncOnStartup == null ? false : enableLookupSyncOnStartup;
  }

  public String getSnapshotWorkingDir()
  {
    return snapshotWorkingDir;
  }

  public int getNumLookupLoadingThreads()
  {
    return numLookupLoadingThreads;
  }

  public boolean getEnableLookupSyncOnStartup()
  {
    return enableLookupSyncOnStartup;
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LookupConfig)) {
      return false;
    }

    LookupConfig that = (LookupConfig) o;

    return getSnapshotWorkingDir().equals(that.getSnapshotWorkingDir());

  }

  @Override
  public String toString()
  {
    return "LookupConfig{" +
           "snapshotWorkingDir='" + getSnapshotWorkingDir() + '\'' +
           " numLookupLoadingThreads='" + getNumLookupLoadingThreads() + '\'' +
           " disableLookupSync='" + getEnableLookupSyncOnStartup() + '\'' +
           '}';
  }
}
