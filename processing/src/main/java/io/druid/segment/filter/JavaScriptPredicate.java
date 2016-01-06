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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ScriptableObject;

/**
 */
abstract class JavaScriptPredicate implements Predicate<Object[]>
{
  final ScriptableObject scope;
  final org.mozilla.javascript.Function fnApply;
  final String script;

  public JavaScriptPredicate(final String script)
  {
    Preconditions.checkNotNull(script, "script must not be null");
    this.script = script;

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
  public boolean apply(final Object[] input)
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

  abstract boolean applyInContext(Context cx, Object[] input);

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JavaScriptPredicate that = (JavaScriptPredicate) o;

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

  @Override
  public String toString()
  {
    return "JavaScriptPredicate{" +
           "script='" + script + '\'' +
           '}';
  }
}
