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

package org.apache.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ValueType;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JavaScriptPostAggregator implements PostAggregator
{
  private interface Function
  {
    double apply(Object[] args);
  }

  private static Function compile(String function)
  {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(JavaScriptConfig.DEFAULT_OPTIMIZATION_LEVEL);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();


    return new Function()
    {
      @Override
      public double apply(Object[] args)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        return Context.toNumber(fn.call(cx, scope, scope, args));
      }
    };
  }

  private final String name;
  private final List<String> fieldNames;
  private final String function;
  private final JavaScriptConfig config;

  /**
   * The field is declared volatile in order to ensure safe publication of the object
   * in {@link #compile(String)} without worrying about final modifiers
   * on the fields of the created object
   *
   * @see <a href="https://github.com/apache/druid/pull/6662#discussion_r237013157">
   *     https://github.com/apache/druid/pull/6662#discussion_r237013157</a>
   */
  @MonotonicNonNull
  private volatile Function fn;

  @JsonCreator
  public JavaScriptPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("function") final String function,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator name");
    Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");
    Preconditions.checkNotNull(function, "Must have a valid, non-null function");

    this.name = name;
    this.fieldNames = fieldNames;
    this.function = function;
    this.config = config;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldNames);
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    Function fn = getCompiledScript();
    final Object[] args = new Object[fieldNames.size()];
    int i = 0;
    for (String field : fieldNames) {
      args[i++] = combinedAggregators.get(field);
    }
    return fn.apply(args);
  }

  /**
   * {@link #compute} can be called by multiple threads, so this function should be thread-safe to avoid extra
   * script compilation.
   */
  @EnsuresNonNull("fn")
  private Function getCompiledScript()
  {
    // JavaScript configuration should be checked when it's actually used because someone might still want Druid
    // nodes to be able to deserialize JavaScript-based objects even though JavaScript is disabled.
    Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");

    Function syncedFn = fn;
    if (syncedFn == null) {
      synchronized (config) {
        syncedFn = fn;
        if (syncedFn == null) {
          syncedFn = compile(function);
          fn = syncedFn;
        }
      }
    }
    return syncedFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.JAVA_SCRIPT)
        .appendStrings(fieldNames)
        .appendString(function)
        .build();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public ValueType getType()
  {
    return ValueType.DOUBLE;
  }

  @Override
  public JavaScriptPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
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

    JavaScriptPostAggregator that = (JavaScriptPostAggregator) o;

    if (!fieldNames.equals(that.fieldNames)) {
      return false;
    }
    if (!function.equals(that.function)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldNames.hashCode();
    result = 31 * result + function.hashCode();
    return result;
  }
}
