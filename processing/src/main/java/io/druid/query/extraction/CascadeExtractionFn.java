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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

import javax.annotation.Nullable;
import java.util.Arrays;

public class CascadeExtractionFn implements ExtractionFn
{
  private final ExtractionFn extractionFns[];
  private final ChainedExtractionFn chainedExtractionFn;
  private final ChainedExtractionFn DEFAULT_CHAINED_EXTRACTION_FN = new ChainedExtractionFn(
      new ExtractionFn()
      {
        @Override
        public byte[] getCacheKey()
        {
          return new byte[0];
        }

        @Nullable
        @Override
        public String apply(@Nullable Object value)
        {
          return null;
        }

        @Nullable
        @Override
        public String apply(@Nullable String value)
        {
          return null;
        }

        @Override
        public String apply(long value)
        {
          return null;
        }

        @Override
        public boolean preservesOrdering()
        {
          return false;
        }

        @Override
        public ExtractionType getExtractionType()
        {
          return ExtractionType.MANY_TO_ONE;
        }

        @Override
        public String toString()
        {
          return "nullExtractionFn{}";
        }
      },
      null
  );

  @JsonCreator
  public CascadeExtractionFn(
      @JsonProperty("extractionFns") ExtractionFn[] extractionFn
  )
  {
    Preconditions.checkArgument(extractionFn != null, "extractionFns should not be null");
    this.extractionFns = extractionFn;
    if (extractionFns.length == 0) {
      this.chainedExtractionFn = DEFAULT_CHAINED_EXTRACTION_FN;
    } else {
      ChainedExtractionFn root = null;
      for (ExtractionFn fn : extractionFn) {
        Preconditions.checkArgument(fn != null, "empty function is not allowed");
        root = new ChainedExtractionFn(fn, root);
      }
      this.chainedExtractionFn = root;
    }
  }

  @JsonProperty
  public ExtractionFn[] getExtractionFns()
  {
    return extractionFns;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] cacheKey = new byte[]{ExtractionCacheHelper.CACHE_TYPE_ID_CASCADE};

    return Bytes.concat(cacheKey, chainedExtractionFn.getCacheKey());
  }

  @Override
  @Nullable
  public String apply(@Nullable Object value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  public String apply(long value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return chainedExtractionFn.preservesOrdering();
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return chainedExtractionFn.getExtractionType();
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

    CascadeExtractionFn that = (CascadeExtractionFn) o;

    if (!Arrays.equals(extractionFns, that.extractionFns)) {
      return false;
    }
    if (!chainedExtractionFn.equals(that.chainedExtractionFn)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return chainedExtractionFn.hashCode();
  }

  @Override
  public String toString()
  {
    return "CascadeExtractionFn{" +
           "extractionFns=[" + chainedExtractionFn.toString() + "]}";
  }

  private static class ChainedExtractionFn
  {
    private final ExtractionFn fn;
    private final ChainedExtractionFn child;

    public ChainedExtractionFn(ExtractionFn fn, ChainedExtractionFn child)
    {
      this.fn = fn;
      this.child = child;
    }

    public byte[] getCacheKey()
    {
      byte[] fnCacheKey = fn.getCacheKey();

      return (child != null) ? Bytes.concat(fnCacheKey, child.getCacheKey()) : fnCacheKey;
    }

    @Nullable
    public String apply(@Nullable Object value)
    {
      return fn.apply((child != null) ? child.apply(value) : value);
    }

    @Nullable
    public String apply(@Nullable String value)
    {
      return fn.apply((child != null) ? child.apply(value) : value);
    }

    public String apply(long value)
    {
      return fn.apply((child != null) ? child.apply(value) : value);
    }

    public boolean preservesOrdering()
    {
      boolean childPreservesOrdering = (child == null) || child.preservesOrdering();
      return fn.preservesOrdering() && childPreservesOrdering;
    }

    public ExtractionType getExtractionType()
    {
      if (child != null && child.getExtractionType() == ExtractionType.MANY_TO_ONE) {
        return ExtractionType.MANY_TO_ONE;
      } else {
        return fn.getExtractionType();
      }
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

      ChainedExtractionFn that = (ChainedExtractionFn) o;

      if (!fn.equals(that.fn)) {
        return false;
      }
      if (child != null && !child.equals(that.child)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = fn.hashCode();
      if (child != null) {
        result = 31 * result + child.hashCode();
      }
      return result;
    }

    @Override
    public String toString()
    {
      return (child != null)
             ? Joiner.on(",").join(child.toString(), fn.toString())
             : fn.toString();
    }
  }
}
