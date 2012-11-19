/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.aggregation;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.metamx.common.IAE;
import com.metamx.druid.processing.FloatMetricSelector;
import com.metamx.druid.processing.MetricSelectorFactory;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.ScriptableObject;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

public class JavaScriptAggregatorFactory implements AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x6;

  private final String name;
  private final List<String> fieldNames;
  private final String script;

  private final JavaScriptAggregator.ScriptAggregator combiner;

  @JsonCreator
  public JavaScriptAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("script") final String expression
  )
  {
    this.name = name;
    this.script = expression;
    this.fieldNames = fieldNames;
    this.combiner = compileScript(script);
  }

  @Override
  public Aggregator factorize(final MetricSelectorFactory metricFactory)
  {
    return new JavaScriptAggregator(
        name,
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, FloatMetricSelector>()
            {
              @Override
              public FloatMetricSelector apply(@Nullable String s) { return metricFactory.makeFloatMetricSelector(s); }
            }
        ),
        compileScript(script)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(final MetricSelectorFactory metricFactory)
  {
    return new JavaScriptBufferAggregator(
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, FloatMetricSelector>()
            {
              @Override
              public FloatMetricSelector apply(@Nullable String s)
              {
                return metricFactory.makeFloatMetricSelector(s);
              }
            }
        ),
        compileScript(script)
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
    return combiner.combine(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object deserialize(Object object)
  {
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
  public List<String> getFieldNames() {
    return fieldNames;
  }

  @JsonProperty
  public String getScript() {
    return script;
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
      byte[] fieldNameBytes = Joiner.on(",").join(fieldNames).getBytes();
      byte[] sha1 = md.digest(script.getBytes());

      return ByteBuffer.allocate(1 + fieldNameBytes.length + sha1.length)
                       .put(CACHE_TYPE_ID)
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
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return combiner.reset();
  }

  @Override
  public String toString()
  {
    return "JavaScriptAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames=" + fieldNames +
           ", script='" + script + '\'' +
           '}';
  }

  protected static Function getScriptFunction(String name, ScriptableObject scope)
  {
    Object fun = scope.get(name, scope);
    if (fun instanceof Function) {
      return (Function) fun;
    } else {
      throw new IAE("Function [%s] not defined in script", name);
    }
  }

  public static JavaScriptAggregator.ScriptAggregator compileScript(final String script)
  {
    final Context cx = Context.enter();
    cx.setOptimizationLevel(9);
    final ScriptableObject scope = cx.initStandardObjects();

    Script compiledScript = cx.compileString(script, "script", 1, null);
    compiledScript.exec(cx, scope);

    final Function fnAggregate = getScriptFunction("aggregate", scope);
    final Function fnReset = getScriptFunction("reset", scope);
    final Function fnCombine = getScriptFunction("combine", scope);

    return new JavaScriptAggregator.ScriptAggregator()
    {
      @Override
      public double aggregate(double current, FloatMetricSelector[] selectorList)
      {
        final int size = selectorList.length;
        final Object[] args = new Object[size + 1];

        args[0] = current;
        int i = 0;
        while (i < size) {
          args[i + 1] = selectorList[i++].get();
        }

        Object res = fnAggregate.call(cx, scope, scope, args);
        return Context.toNumber(res);
      }

      @Override
      public double combine(double a, double b)
      {
        Object res = fnCombine.call(cx, scope, scope, new Object[]{a, b});
        return Context.toNumber(res);
      }

      @Override
      public double reset()
      {
        Object res = fnReset.call(cx, scope, scope, new Object[]{});
        return Context.toNumber(res);
      }

      @Override
      protected void finalize() throws Throwable
      {
        cx.exit();
        super.finalize();
      }
    };
  }
}
