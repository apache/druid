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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
import io.druid.js.JavaScriptConfig;
import io.druid.segment.BaseObjectColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextAction;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JavaScriptAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final List<String> fieldNames;
  private final String fnAggregate;
  private final String fnReset;
  private final String fnCombine;
  private final JavaScriptConfig config;

  // This variable is lazily initialized to avoid unnecessary JavaScript compilation during JSON serde
  private JavaScriptAggregator.ScriptAggregator compiledScript;

  @JsonCreator
  public JavaScriptAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fnAggregate") final String fnAggregate,
      @JsonProperty("fnReset") final String fnReset,
      @JsonProperty("fnCombine") final String fnCombine,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");
    Preconditions.checkNotNull(fnAggregate, "Must have a valid, non-null fnAggregate");
    Preconditions.checkNotNull(fnReset, "Must have a valid, non-null fnReset");
    Preconditions.checkNotNull(fnCombine, "Must have a valid, non-null fnCombine");

    this.name = name;
    this.fieldNames = fieldNames;

    this.fnAggregate = fnAggregate;
    this.fnReset = fnReset;
    this.fnCombine = fnCombine;
    this.config = config;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    checkAndCompileScript();
    return new JavaScriptAggregator(
        fieldNames.stream().map(columnFactory::makeColumnValueSelector).collect(Collectors.toList()),
        compiledScript
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    checkAndCompileScript();
    return new JavaScriptBufferAggregator(
        fieldNames.stream().map(columnSelectorFactory::makeColumnValueSelector).collect(Collectors.toList()),
        compiledScript
    );
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    checkAndCompileScript();
    return compiledScript.combine(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new DoubleAggregateCombiner()
    {
      private double combined;

      @Override
      public void reset(ColumnValueSelector selector)
      {
        combined = selector.getDouble();
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        checkAndCompileScript();
        combined = compiledScript.combine(combined, selector.getDouble());
      }

      @Override
      public double getDouble()
      {
        return combined;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new JavaScriptAggregatorFactory(name, Collections.singletonList(name), fnCombine, fnReset, fnCombine, config);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other.getClass() == this.getClass()) {
      JavaScriptAggregatorFactory castedOther = (JavaScriptAggregatorFactory) other;
      if (this.fnCombine.equals(castedOther.fnCombine) && this.fnReset.equals(castedOther.fnReset)) {
        return getCombiningFactory();
      }
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return ImmutableList.copyOf(
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, AggregatorFactory>()
            {
              @Override
              public AggregatorFactory apply(String input)
              {
                return new JavaScriptAggregatorFactory(input, Collections.singletonList(input), fnCombine, fnReset, fnCombine, config);
              }
            }
        )
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getFnAggregate()
  {
    return fnAggregate;
  }

  @JsonProperty
  public String getFnReset()
  {
    return fnReset;
  }

  @JsonProperty
  public String getFnCombine()
  {
    return fnCombine;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldNames;
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] fieldNameBytes = StringUtils.toUtf8(Joiner.on(",").join(fieldNames));
      byte[] sha1 = md.digest(StringUtils.toUtf8(fnAggregate + fnReset + fnCombine));

      return ByteBuffer.allocate(1 + fieldNameBytes.length + sha1.length)
                       .put(AggregatorUtil.JS_CACHE_TYPE_ID)
                       .put(fieldNameBytes)
                       .put(sha1)
                       .array();
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to get SHA1 digest instance", e);
    }
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES;
  }

  @Override
  public String toString()
  {
    return "JavaScriptAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames=" + fieldNames +
           ", fnAggregate='" + fnAggregate + '\'' +
           ", fnReset='" + fnReset + '\'' +
           ", fnCombine='" + fnCombine + '\'' +
           '}';
  }

  /**
   * This class can be used by multiple threads, so this function should be thread-safe to avoid extra
   * script compilation.
   */
  private void checkAndCompileScript()
  {
    if (compiledScript == null) {
      // JavaScript configuration should be checked when it's actually used because someone might still want Druid
      // nodes to be able to deserialize JavaScript-based objects even though JavaScript is disabled.
      Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");

      synchronized (config) {
        if (compiledScript == null) {
          compiledScript = compileScript(fnAggregate, fnReset, fnCombine);
        }
      }
    }
  }

  @VisibleForTesting
  static JavaScriptAggregator.ScriptAggregator compileScript(
      final String aggregate,
      final String reset,
      final String combine
  )
  {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    Context context = contextFactory.enterContext();
    context.setOptimizationLevel(JavaScriptConfig.DEFAULT_OPTIMIZATION_LEVEL);

    final ScriptableObject scope = context.initStandardObjects();

    final Function fnAggregate = context.compileFunction(scope, aggregate, "aggregate", 1, null);
    final Function fnReset = context.compileFunction(scope, reset, "reset", 1, null);
    final Function fnCombine = context.compileFunction(scope, combine, "combine", 1, null);
    Context.exit();

    return new JavaScriptAggregator.ScriptAggregator()
    {
      @Override
      public double aggregate(final double current, final BaseObjectColumnValueSelector[] selectorList)
      {
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();

          // Disable primitive wrapping- we want Java strings and primitives to behave like JS entities.
          cx.getWrapFactory().setJavaPrimitiveWrap(false);
        }

        final int size = selectorList.length;
        final Object[] args = new Object[size + 1];

        args[0] = current;
        for (int i = 0; i < size; i++) {
          final BaseObjectColumnValueSelector selector = selectorList[i];
          if (selector != null) {
            final Object arg = selector.getObject();
            if (arg != null && arg.getClass().isArray()) {
              // Context.javaToJS on an array sort of works, although it returns false for Array.isArray(...) and
              // may have other issues too. Let's just copy the array and wrap that.
              final Object[] arrayAsObjectArray = new Object[Array.getLength(arg)];
              for (int j = 0; j < Array.getLength(arg); j++) {
                arrayAsObjectArray[j] = Array.get(arg, j);
              }
              args[i + 1] = cx.newArray(scope, arrayAsObjectArray);
            } else {
              args[i + 1] = Context.javaToJS(arg, scope);
            }
          }
        }

        final Object res = fnAggregate.call(cx, scope, scope, args);
        return Context.toNumber(res);
      }

      @Override
      public double combine(final double a, final double b)
      {
        final Object res = contextFactory.call(
            new ContextAction()
            {
              @Override
              public Object run(final Context cx)
              {
                return fnCombine.call(cx, scope, scope, new Object[]{a, b});
              }
            }
        );
        return Context.toNumber(res);
      }

      @Override
      public double reset()
      {
        final Object res = contextFactory.call(
            new ContextAction()
            {
              @Override
              public Object run(final Context cx)
              {
                return fnReset.call(cx, scope, scope, new Object[]{});
              }
            }
        );
        return Context.toNumber(res);
      }

      @Override
      public void close()
      {
        if (Context.getCurrentContext() != null) {
          Context.exit();
        }
      }
    };
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
    JavaScriptAggregatorFactory that = (JavaScriptAggregatorFactory) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(fieldNames, that.fieldNames) &&
           Objects.equals(fnAggregate, that.fnAggregate) &&
           Objects.equals(fnReset, that.fnReset) &&
           Objects.equals(fnCombine, that.fnCombine);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldNames, fnAggregate, fnReset, fnCombine);
  }
}
