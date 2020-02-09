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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;

/**
 * The logical "not" operator for the "having" clause.
 */
public class NotHavingSpec implements HavingSpec
{
  private final HavingSpec havingSpec;

  @JsonCreator
  public NotHavingSpec(@JsonProperty("havingSpec") HavingSpec havingSpec)
  {
    this.havingSpec = havingSpec;
  }

  @JsonProperty("havingSpec")
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @Override
  public void setQuery(GroupByQuery query)
  {
    havingSpec.setQuery(query);
  }

  @Override
  public boolean eval(ResultRow row)
  {
    return !havingSpec.eval(row);
  }

  @Override
  public String toString()
  {
    return "NotHavingSpec{" +
           "havingSpec=" + havingSpec +
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

    NotHavingSpec that = (NotHavingSpec) o;

    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpec != null ? havingSpec.hashCode() : 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(HavingSpecUtil.CACHE_TYPE_ID_NOT)
        .appendCacheable(havingSpec)
        .build();
  }
}
