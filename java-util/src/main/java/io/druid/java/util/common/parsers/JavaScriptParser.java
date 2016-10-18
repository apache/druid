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

package io.druid.java.util.common.parsers;

import com.google.common.base.Function;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.util.List;
import java.util.Map;

/**
 */
public class JavaScriptParser implements Parser<String, Object>
{
  private static Function<Object, Object> compile(String function)
  {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();

    return new Function<Object, Object>()
    {
      public Object apply(Object input)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        final Object res = fn.call(cx, scope, scope, new Object[]{input});
        return res != null ? Context.toObject(res, scope) : null;
      }
    };
  }

  private final Function<Object, Object> fn;

  public JavaScriptParser(
      final String function
  )
  {
    this.fn = compile(function);
  }

  public Function<Object, Object> getFn()
  {
    return fn;
  }

  @Override
  public Map<String, Object> parse(String input)
  {
    try {
      final Object compiled = fn.apply(input);
      if (!(compiled instanceof Map)) {
        throw new ParseException("JavaScript parsed value must be in {key: value} format!");
      }

      return (Map) compiled;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getFieldNames()
  {
    throw new UnsupportedOperationException();
  }
}
