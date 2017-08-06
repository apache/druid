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

package io.druid.query.extraction;

import com.google.common.base.Strings;

import javax.annotation.Nullable;

public class IdentityExtractionFn implements ExtractionFn
{
  private static final IdentityExtractionFn instance = new IdentityExtractionFn();

  private IdentityExtractionFn()
  {

  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{ExtractionCacheHelper.CACHE_TYPE_ID_IDENTITY};
  }

  @Override
  @Nullable
  public String apply(@Nullable Object value)
  {
    return value == null ? null : Strings.emptyToNull(value.toString());
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
  {
    return Strings.emptyToNull(value);
  }

  @Override
  public String apply(long value)
  {
    return Long.toString(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return true;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.ONE_TO_ONE;
  }

  @Override
  public String toString()
  {
     return "Identity";
  }

  @Override
  public boolean equals(Object o)
  {
     return o != null && o instanceof IdentityExtractionFn;
  }

  public static final IdentityExtractionFn getInstance()
  {
    return instance;
  }
}
