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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.RangeSet;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.JavaScriptFilter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import java.nio.ByteBuffer;

public class JavaScriptDimFilter implements DimFilter
{
  private final String dimension;
  private final String function;
  private final ExtractionFn extractionFn;
  private final JavaScriptConfig config;

  private final JavaScriptPredicateFactory predicateFactory;

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

    if (config.isEnabled()) {
      this.predicateFactory = new JavaScriptPredicateFactory(function, extractionFn);
    } else {
      this.predicateFactory = null;
    }
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
    if (!config.isEnabled()) {
      throw new ISE("JavaScript is disabled");
    }

    return new JavaScriptFilter(dimension, predicateFactory);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
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
