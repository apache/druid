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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RegisteredLookupExtractionFn implements ExtractionFn
{
  // Protected for moving to not-null by `delegateLock`
  private volatile LookupExtractionFn delegate = null;
  private final Object delegateLock = new Object();
  private final LookupExtractorFactoryContainerProvider manager;
  private final String lookup;
  private final boolean retainMissingValue;
  private final String replaceMissingValueWith;
  private final Boolean injective;
  private final boolean optimize;

  @JsonCreator
  public RegisteredLookupExtractionFn(
      @JacksonInject LookupExtractorFactoryContainerProvider manager,
      @JsonProperty("lookup") String lookup,
      @JsonProperty("retainMissingValue") final boolean retainMissingValue,
      @JsonProperty("replaceMissingValueWith") @Nullable final String replaceMissingValueWith,
      @JsonProperty("injective") @Nullable final Boolean injective,
      @JsonProperty("optimize") Boolean optimize
  )
  {
    Preconditions.checkArgument(lookup != null, "`lookup` required");
    this.manager = manager;
    this.replaceMissingValueWith = replaceMissingValueWith;
    this.retainMissingValue = retainMissingValue;
    this.injective = injective;
    this.optimize = optimize == null ? true : optimize;
    this.lookup = lookup;
  }

  @JsonProperty("lookup")
  public String getLookupName()
  {
    return lookup;
  }

  @JsonProperty("retainMissingValue")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isRetainMissingValue()
  {
    return retainMissingValue;
  }

  @Nullable
  @JsonProperty("replaceMissingValueWith")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getReplaceMissingValueWith()
  {
    return replaceMissingValueWith;
  }

  @Nullable
  @JsonProperty("injective")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean isInjective()
  {
    return injective;
  }

  @JsonProperty("optimize")
  public boolean isOptimize()
  {
    return optimize;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] keyPrefix = StringUtils.toUtf8(getClass().getName());
    final byte[] lookupName = StringUtils.toUtf8(getLookupName());
    final byte[] delegateKey = ensureDelegate().getCacheKey();
    return ByteBuffer
        .allocate(keyPrefix.length + 1 + lookupName.length + 1 + delegateKey.length)
        .put(keyPrefix).put((byte) 0xFF)
        .put(lookupName).put((byte) 0xFF)
        .put(delegateKey)
        .array();
  }

  @Override
  @Nullable
  public String apply(@Nullable Object value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  public String apply(long value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return ensureDelegate().preservesOrdering();
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ensureDelegate().getExtractionType();
  }

  /**
   * Return the equivalent {@link LookupExtractionFn}, with {@link LookupExtractor} resolved.
   */
  public LookupExtractionFn getDelegate()
  {
    return ensureDelegate();
  }

  private LookupExtractionFn ensureDelegate()
  {
    if (null == delegate) {
      // http://www.javamex.com/tutorials/double_checked_locking.shtml
      synchronized (delegateLock) {
        if (null == delegate) {
          final LookupExtractor lookupExtractor =
              manager.get(getLookupName())
                     .orElseThrow(() -> new ISE("Lookup [%s] not found", getLookupName()))
                     .getLookupExtractorFactory()
                     .get();

          delegate = new LookupExtractionFn(
              lookupExtractor,
              retainMissingValue,
              replaceMissingValueWith,
              injective,
              optimize
          );
        }
      }
    }
    return delegate;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RegisteredLookupExtractionFn that = (RegisteredLookupExtractionFn) o;
    return retainMissingValue == that.retainMissingValue &&
           optimize == that.optimize &&
           Objects.equals(lookup, that.lookup) &&
           Objects.equals(replaceMissingValueWith, that.replaceMissingValueWith) &&
           Objects.equals(injective, that.injective);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
  }

  @Override
  public String toString()
  {
    return "RegisteredLookupExtractionFn{" +
           "delegate=" + delegate +
           ", lookup='" + lookup + '\'' +
           ", retainMissingValue=" + retainMissingValue +
           ", replaceMissingValueWith='" + replaceMissingValueWith + '\'' +
           ", injective=" + injective +
           ", optimize=" + optimize +
           '}';
  }
}
