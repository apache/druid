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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.nio.ByteBuffer;

public class JavascriptDimExtractionFn implements DimExtractionFn
{
  private static Function<String, String> compile(String function) {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();


    return new Function<String, String>()
    {
      public String apply(String input)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        final Object res = fn.call(cx, scope, scope, new String[]{input});
        return res != null ? Context.toString(res) : null;
      }
    };
  }

  private static final byte CACHE_TYPE_ID = 0x4;

  private final String function;
  private final Function<String, String> fn;

  @JsonCreator
  public JavascriptDimExtractionFn(
      @JsonProperty("function") String function
  )
  {
    this.function = function;
    this.fn = compile(function);
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] bytes = function.getBytes(Charsets.UTF_8);
    return ByteBuffer.allocate(1 + bytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(bytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    return fn.apply(dimValue);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "JavascriptDimExtractionFn{" +
           "function='" + function + '\'' +
           '}';
  }
}
