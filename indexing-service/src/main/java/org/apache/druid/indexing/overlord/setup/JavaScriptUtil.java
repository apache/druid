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

package org.apache.druid.indexing.overlord.setup;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavaScriptUtil
{
  public static <T> T compileSelectorFunction(Class<T> interfaceClass, String function)
  {
    final ScriptEngine engine = new ScriptEngineManager().getEngineByName("js");
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    bindings.put("polyglot.js.nashorn-compat", true);
    try {
      ((Compilable) engine).compile("var apply = " + function).eval();
      return ((Invocable) engine).getInterface(interfaceClass);
    }
    catch (ScriptException e) {
      throw new RuntimeException(e);
    }
  }
}
