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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

/**
 * A user configuration holder for all query types.
 * Any query-specific configurations should go to their own configuration.
 * @see org.apache.druid.query.groupby.GroupByQueryConfig
 * @see org.apache.druid.query.search.SearchQueryConfig
 * @see org.apache.druid.query.topn.TopNQueryConfig
 * @see org.apache.druid.query.metadata.SegmentMetadataQueryConfig
 * @see org.apache.druid.query.scan.ScanQueryConfig
 *
 * This class config map is populated by any runtime property prefixed with druid.query.override.default.context
 * Note that config values should not be directly retrieved from this class but instead should
 * be read through {@link QueryContexts}. This class contains configs from runtime property which is then merged with
 * configs passed in query context. The result of the merge is subsequently stored in the query context.
 * The order of precedence in merging of the configs is as follow:
 * runtime property values (store in this class) override by query context parameter passed in with the query
 *

 */
public class OverrideDefaultQueryContext
{
  private Map<String, Object> configs = new HashMap<>();

  @JsonAnyGetter
  public Map<String, Object> getConfigs()
  {
    return configs;
  }

  public void setConfigs(Map<String, Object> configs)
  {
    this.configs = configs;
  }

  @JsonAnySetter
  public void setConfig(final String name, final Object value)
  {
    this.configs.put(name, value);
  }
}
