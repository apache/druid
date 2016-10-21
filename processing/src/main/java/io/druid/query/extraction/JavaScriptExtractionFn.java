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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.js.JavaScriptConfig;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.nio.ByteBuffer;

public class JavaScriptExtractionFn implements ExtractionFn
{
  private static Function<Object, String> compile(String function)
  {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(JavaScriptConfig.DEFAULT_OPTIMIZATION_LEVEL);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();


    return new Function<Object, String>()
    {
      public String apply(Object input)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        final Object res = fn.call(cx, scope, scope, new Object[]{input});
        return res != null ? Context.toString(res) : null;
      }
    };
  }

  private final String function;
  private final Function<Object, String> fn;
  private final boolean injective;

  @JsonCreator
  public JavaScriptExtractionFn(
      @JsonProperty("function") String function,
      @JsonProperty("injective") boolean injective,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkNotNull(function, "function must not be null");

    this.function = function;
    this.injective = injective;

    if (config.isDisabled()) {
      this.fn = null;
    } else {
      this.fn = compile(function);
    }
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @JsonProperty
  public boolean isInjective()
  {
    return this.injective;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] bytes = StringUtils.toUtf8(function);
    return ByteBuffer.allocate(1 + bytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_JAVASCRIPT)
                     .put(bytes)
                     .array();
  }

  @Override
  public String apply(Object value)
  {
    if (fn == null) {
      throw new ISE("JavaScript is disabled");
    }

    return Strings.emptyToNull(fn.apply(value));
  }

  @Override
  public String apply(String value)
  {
    return this.apply((Object) Strings.emptyToNull(value));
  }

  @Override
  public String apply(long value)
  {
    return this.apply((Long) value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return injective ? ExtractionType.ONE_TO_ONE : ExtractionType.MANY_TO_ONE;
  }

  @Override
  public String toString()
  {
    return "JavascriptDimExtractionFn{" +
           "function='" + function + '\'' +
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

    JavaScriptExtractionFn that = (JavaScriptExtractionFn) o;

    if (!function.equals(that.function)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return function.hashCode();
  }
}
