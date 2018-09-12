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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.JavaScriptFilter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class JavaScriptDimFilter implements DimFilter
{
  private final String dimension;
  private final String function;
  private final ExtractionFn extractionFn;
  private final JavaScriptConfig config;

  // This variable is lazily initialized to avoid unnecessary JavaScript compilation during JSON serde
  private JavaScriptPredicateFactory predicateFactory;

  @JsonCreator
  public JavaScriptDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("function") String function,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimension = dimension;
    this.function = function;
    this.extractionFn = extractionFn;
    this.config = config;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] functionBytes = StringUtils.toUtf8(function);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + functionBytes.length + extractionFnBytes.length)
                     .put(DimFilterUtils.JAVASCRIPT_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(functionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    checkAndCreatePredicateFactory();
    return new JavaScriptFilter(dimension, predicateFactory);
  }

  /**
   * This class can be used by multiple threads, so this function should be thread-safe to avoid extra
   * script compilation.
   */
  private void checkAndCreatePredicateFactory()
  {
    if (predicateFactory == null) {
      // JavaScript configuration should be checked when it's actually used because someone might still want Druid
      // nodes to be able to deserialize JavaScript-based objects even though JavaScript is disabled.
      Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");

      synchronized (config) {
        if (predicateFactory == null) {
          predicateFactory = new JavaScriptPredicateFactory(function, extractionFn);
        }
      }
    }
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public HashSet<String> getRequiredColumns()
  {
    return Sets.newHashSet(dimension);
  }

  @Override
  public String toString()
  {
    return "JavaScriptDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", function='" + function + '\'' +
           ", extractionFn='" + extractionFn + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JavaScriptDimFilter)) {
      return false;
    }

    JavaScriptDimFilter that = (JavaScriptDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!function.equals(that.function)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + function.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  public static class JavaScriptPredicateFactory implements DruidPredicateFactory
  {
    final ScriptableObject scope;
    final Function fnApply;
    final String script;
    final ExtractionFn extractionFn;

    public JavaScriptPredicateFactory(final String script, final ExtractionFn extractionFn)
    {
      Preconditions.checkNotNull(script, "script must not be null");
      this.script = script;
      this.extractionFn = extractionFn;

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();

        fnApply = cx.compileFunction(scope, script, "script", 1, null);
      }
      finally {
        Context.exit();
      }
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      return input -> applyObject(input);
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return input -> applyObject(input);
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return input -> applyObject(input);
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return input -> applyObject(input);
    }

    public boolean applyObject(final Object input)
    {
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return applyInContext(cx, input);
      }
      finally {
        Context.exit();
      }
    }

    public boolean applyInContext(Context cx, Object input)
    {
      if (extractionFn != null) {
        input = extractionFn.apply(input);
      }
      return Context.toBoolean(fnApply.call(cx, scope, scope, new Object[]{input}));
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

      JavaScriptPredicateFactory that = (JavaScriptPredicateFactory) o;

      if (!script.equals(that.script)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return script.hashCode();
    }
  }
}
