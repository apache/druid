/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.ValueAccessor;

import javax.annotation.Nullable;

public class RegisteredLookupExtractionFn implements ExtractionFn
{
  private final LookupExtractionFn delegate;
  private final String name;

  RegisteredLookupExtractionFn(LookupExtractionFn delegate, String name)
  {
    this.delegate = delegate;
    this.name = name;
  }

  @JsonCreator
  public static RegisteredLookupExtractionFn create(
      @JacksonInject LookupReferencesManager manager,
      @JsonProperty("lookup") String lookup,
      @JsonProperty("retainMissingValue") final boolean retainMissingValue,
      @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
      @JsonProperty("injective") final boolean injective,
      @JsonProperty("optimize") Boolean optimize
  )
  {
    Preconditions.checkArgument(lookup != null, "`lookup` required");
    final LookupExtractorFactory factory = manager.get(lookup);
    Preconditions.checkNotNull(factory, "lookup [%s] not found", lookup);
    return new RegisteredLookupExtractionFn(
        new LookupExtractionFn(
            factory.get(),
            retainMissingValue,
            replaceMissingValueWith,
            injective,
            optimize
        ),
        lookup
    );
  }

  @JsonProperty("lookup")
  public String getLookup()
  {
    return name;
  }

  @JsonProperty("retainMissingValue")
  public boolean isRetainMissingValue()
  {
    return delegate.isRetainMissingValue();
  }

  @JsonProperty("replaceMissingValueWith")
  public String getReplaceMissingValueWith()
  {
    return delegate.getReplaceMissingValueWith();
  }

  @JsonProperty("injective")
  public boolean isInjective()
  {
    return delegate.isInjective();
  }

  @JsonProperty("optimize")
  public boolean isOptimize()
  {
    return delegate.isOptimize();
  }

  @Override
  public byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }

  @Override
  public void init(ValueAccessor accessor)
  {
    delegate.init(accessor);
  }

  @Override
  public String apply(Object value)
  {
    return delegate.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return delegate.preservesOrdering();
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return delegate.getExtractionType();
  }

  @Override
  public String toString()
  {
    return "RegisteredLookupExtractionFn{" +
           "delegate=" + delegate +
           ", name='" + name + '\'' +
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

    RegisteredLookupExtractionFn that = (RegisteredLookupExtractionFn) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }
    return name.equals(that.name);

  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }
}
