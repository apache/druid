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

package org.apache.druid.indextable.loader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indextable.loader.config.IndexedTableConfig;
import org.apache.druid.indextable.loader.config.IndexedTableLoaderConfig;
import org.apache.druid.java.util.common.ISE;

import java.io.File;
import java.io.IOException;
import java.util.Map;

class IndexedTableLoaderProvider implements Provider<Map<String, IndexedTableConfig>>
{
  private final String configPath;
  private final ObjectMapper mapper;

  @Inject
  IndexedTableLoaderProvider(IndexedTableLoaderConfig config, @Json ObjectMapper mapper)
  {
    configPath = config.getConfigFilePath();
    this.mapper = mapper;
  }

  @Override
  public Map<String, IndexedTableConfig> get()
  {
    try {
      File configFile = getFile(configPath);
      Map<String, IndexedTableConfig> loadMap =
          mapper.readerFor(new TypeReference<Map<String, IndexedTableConfig>>() {})
                .readValue(configFile);
      return loadMap;
    }
    catch (IOException e) {
      throw new ISE(e, "Could not parse file [%s]", configPath);
    }
  }

  @VisibleForTesting
  File getFile(String path)
  {
    return new File(configPath);
  }
}
