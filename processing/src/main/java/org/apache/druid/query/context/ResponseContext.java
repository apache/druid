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

package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

import java.io.IOException;
import java.util.Map;

/**
 * The context for storing and passing data between chains of {@link org.apache.druid.query.QueryRunner}s.
 * The context is also transferred between Druid nodes with all the data it contains.
 * All the keys associated with data inside the context should be stored here.
 */
@PublicApi
public abstract class ResponseContext
{
  protected abstract Map<String, Object> getDelegate();

  public static final String CTX_UNCOVERED_INTERVALS = "uncoveredIntervals";
  public static final String CTX_UNCOVERED_INTERVALS_OVERFLOWED = "uncoveredIntervalsOverflowed";
  public static final String CTX_MISSING_SEGMENTS = "missingSegments";
  public static final String CTX_ETAG = "ETag";
  public static final String CTX_QUERY_TOTAL_BYTES_GATHERED = "queryTotalBytesGathered";
  /**
   * This variable indicates when a running query should be expired,
   * and is effective only when 'timeout' of queryContext has a positive value.
   */
  public static final String CTX_TIMEOUT_AT = "timeoutAt";
  public static final String CTX_COUNT = "count";

  public Object put(String key, Object value)
  {
    return getDelegate().put(key, value);
  }

  public Object get(String key)
  {
    return getDelegate().get(key);
  }

  public Object remove(String key)
  {
    return getDelegate().remove(key);
  }

  public void putAll(Map<? extends String, ?> m)
  {
    getDelegate().putAll(m);
  }

  public void putAll(ResponseContext responseContext)
  {
    getDelegate().putAll(responseContext.getDelegate());
  }

  public int size()
  {
    return getDelegate().size();
  }

  public String serialize(ObjectMapper objectMapper)
      throws JsonProcessingException
  {
    return objectMapper.writeValueAsString(getDelegate());
  }

  public static ResponseContext deserialize(String responseContext, ObjectMapper objectMapper) throws IOException
  {
    final Map<String, Object> delegate = objectMapper.readValue(
        responseContext,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    return new ResponseContext()
    {
      @Override
      protected Map<String, Object> getDelegate()
      {
        return delegate;
      }
    };
  }
}
