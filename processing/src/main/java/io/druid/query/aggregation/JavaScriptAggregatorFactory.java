/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextAction;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
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
  private final String fnAggregate;
  private final String fnReset;
  private final String fnCombine;


  private final JavaScriptAggregator.ScriptAggregator compiledScript;

  @JsonCreator
  public JavaScriptAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fnAggregate") final String fnAggregate,
      @JsonProperty("fnReset") final String fnReset,
      @JsonProperty("fnCombine") final String fnCombine
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

    this.compiledScript = compileScript(fnAggregate, fnReset, fnCombine);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    return new JavaScriptAggregator(
        name,
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, ObjectColumnSelector>()
            {
              @Override
              public ObjectColumnSelector apply(@Nullable String s)
              {
                return columnFactory.makeObjectColumnSelector(s);
              }
            }
        ),
        compiledScript
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    return new JavaScriptBufferAggregator(
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, ObjectColumnSelector>()
            {
              @Override
              public ObjectColumnSelector apply(@Nullable String s)
              {
                return columnSelectorFactory.makeObjectColumnSelector(s);
              }
            }
        ),
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
    return compiledScript.combine(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new JavaScriptAggregatorFactory(name, Lists.newArrayList(name), fnCombine, fnReset, fnCombine);
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
      byte[] fieldNameBytes = Joiner.on(",").join(fieldNames).getBytes(Charsets.UTF_8);
      byte[] sha1 = md.digest((fnAggregate + fnReset + fnCombine).getBytes(Charsets.UTF_8));

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
    return compiledScript.reset();
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

  public static JavaScriptAggregator.ScriptAggregator compileScript(
      final String aggregate,
      final String reset,
      final String combine
  )
  {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final Function fnAggregate = context.compileFunction(scope, aggregate, "aggregate", 1, null);
    final Function fnReset = context.compileFunction(scope, reset, "reset", 1, null);
    final Function fnCombine = context.compileFunction(scope, combine, "combine", 1, null);
    Context.exit();

    return new JavaScriptAggregator.ScriptAggregator()
    {
      @Override
      public double aggregate(final double current, final ObjectColumnSelector[] selectorList)
      {
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        final int size = selectorList.length;
        final Object[] args = new Object[size + 1];

        args[0] = current;
        for (int i = 0 ; i < size ; i++) {
          final ObjectColumnSelector selector = selectorList[i];
          if (selector != null) {
            args[i + 1] = selector.get();
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JavaScriptAggregatorFactory that = (JavaScriptAggregatorFactory) o;

    if (compiledScript != null ? !compiledScript.equals(that.compiledScript) : that.compiledScript != null)
      return false;
    if (fieldNames != null ? !fieldNames.equals(that.fieldNames) : that.fieldNames != null) return false;
    if (fnAggregate != null ? !fnAggregate.equals(that.fnAggregate) : that.fnAggregate != null) return false;
    if (fnCombine != null ? !fnCombine.equals(that.fnCombine) : that.fnCombine != null) return false;
    if (fnReset != null ? !fnReset.equals(that.fnReset) : that.fnReset != null) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (fnAggregate != null ? fnAggregate.hashCode() : 0);
    result = 31 * result + (fnReset != null ? fnReset.hashCode() : 0);
    result = 31 * result + (fnCombine != null ? fnCombine.hashCode() : 0);
    result = 31 * result + (compiledScript != null ? compiledScript.hashCode() : 0);
    return result;
  }
}
