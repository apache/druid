/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.druid.sql.calcite;

import org.apache.druid.error.DruidException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface DecoupledIgnore
{
  static enum Modes
  {
    PLAN_MISMATCH(AssertionError.class, "AssertionError: query #"),
    NOT_ENOUGH_RULES(DruidException.class, "not enough rules"),
    CANNOT_CONVERT(DruidException.class, "Cannot convert query parts"),
    TARGET_PERSONA(AssertionError.class, "(is <ADMIN> was <OPERATOR>|is <INVALID_INPUT> was <UNCATEGORIZED>|with message a string containing)"),
    ;

    public Class<? extends Throwable> throwableClass;
    public String regex;

    Modes(Class<? extends Throwable> cl,String regex)
    {
      this.throwableClass = cl;
      this.regex = regex;
    }
  };

  Modes mode() default Modes.NOT_ENOUGH_RULES;

}
