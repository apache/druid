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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LookupSnapshotTaker
{
  private static final Logger LOGGER = new Logger(LookupSnapshotTaker.class);
  private static final String PERSIST_FILE_SUFFIX = "lookupSnapshot.json";

  private final ObjectMapper objectMapper;
  private final File persistDirectory;

  public LookupSnapshotTaker(
      final @Json ObjectMapper jsonMapper,
      String persistDirectory
  )
  {
    this.objectMapper = jsonMapper;
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(persistDirectory),
        "can not work without specifying persistDirectory"
    );
    this.persistDirectory = new File(persistDirectory);
    if (!this.persistDirectory.exists()) {
      Preconditions.checkArgument(this.persistDirectory.mkdirs(), "Oups was not able to create persist directory");
    }
    if (!this.persistDirectory.isDirectory()) {
      throw new ISE("Can only persist to directories, [%s] wasn't a directory", persistDirectory);
    }
  }

  public synchronized List<LookupBean> pullExistingSnapshot(final String tier)
  {
    final File persistFile = getPersistFile(tier);

    List<LookupBean> lookupBeanList;
    try {
      if (!persistFile.isFile()) {
        LOGGER.warn("could not find any snapshot file under working directory [%s]", persistDirectory);
        return Collections.emptyList();
      } else if (persistFile.length() == 0) {
        LOGGER.warn("found empty file no lookups to load from [%s]", persistFile.getAbsolutePath());
        return Collections.emptyList();
      }
      lookupBeanList = objectMapper.readValue(persistFile, new TypeReference<List<LookupBean>>() {});
      return lookupBeanList;
    }
    catch (IOException e) {
      throw new ISE(e, "Exception during reading lookups from [%s]", persistFile.getAbsolutePath());
    }
  }

  public synchronized void takeSnapshot(String tier, List<LookupBean> lookups)
  {
    final File persistFile = getPersistFile(tier);

    try {
      FileUtils.writeAtomically(
          persistFile,
          out -> {
            objectMapper.writeValue(out, lookups);
            return null;
          }
      );
    }
    catch (IOException e) {
      throw new ISE(e, "Exception during serialization of lookups using file [%s]", persistFile.getAbsolutePath());
    }
  }

  @VisibleForTesting
  File getPersistFile(final String tier)
  {
    return new File(persistDirectory, StringUtils.format("%s.%s", tier, PERSIST_FILE_SUFFIX));
  }
}
