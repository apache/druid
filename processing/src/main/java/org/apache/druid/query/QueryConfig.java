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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.segment.QueryableIndexStorageAdapter;

/**
 * A user configuration holder for all query types.
 * Any query-specific configurations should go to their own configuration.
 *
 * @see org.apache.druid.query.groupby.GroupByQueryConfig
 * @see org.apache.druid.query.search.SearchQueryConfig
 * @see org.apache.druid.query.topn.TopNQueryConfig
 * @see org.apache.druid.query.metadata.SegmentMetadataQueryConfig
 * @see org.apache.druid.query.scan.ScanQueryConfig
 */
public class QueryConfig
{
  @JsonProperty
  private Vectorize vectorize = QueryContexts.DEFAULT_VECTORIZE;

  @JsonProperty
  private int vectorSize = QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE;

  public Vectorize getVectorize()
  {
    return vectorize;
  }

  public int getVectorSize()
  {
    return vectorSize;
  }

  public QueryConfig withOverrides(final Query<?> query)
  {
    final QueryConfig newConfig = new QueryConfig();
    newConfig.vectorize = QueryContexts.getVectorize(query, vectorize);
    newConfig.vectorSize = QueryContexts.getVectorSize(query, vectorSize);
    return newConfig;
  }
}
