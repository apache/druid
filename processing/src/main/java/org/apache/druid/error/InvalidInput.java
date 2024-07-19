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

package org.apache.druid.error;

/**
 * A failure type used to make {@link DruidException}s of category
 * {@link DruidException.Category#INVALID_INPUT} for persona {@link DruidException.Persona#USER}.
 */
public class InvalidInput extends BaseFailure
{
  public static DruidException exception(String msg, Object... args)
  {
    return exception(null, msg, args);
  }

  public static DruidException exception(Throwable t, String msg, Object... args)
  {
    return DruidException.fromFailure(new InvalidInput(t, msg, args));
  }

  /**
   * evalues a condition. If it's false, it throws the appropriate DruidException
   *
   * @param condition - boolean condition to validate
   * @param msg - passed through to DruidException.exception()
   * @param args - passed through to DruidException.exception()
   */
  public static void conditionalException(boolean condition, String msg, Object... args)
  {
    if (!condition) {
      throw exception(msg, args);
    }
  }

  /**
   * evalues a condition. If it's false, it throws the appropriate DruidException with a given cause
   *
   * @param condition - boolean condition to validate
   * @param t - throwable to pass to InvalidInput.exception()
   * @param msg - passed through to InvalidInput.exception()
   * @param args - passed through to InvalidInput.exception()
   */

  public static void conditionalException(boolean condition, Throwable t, String msg, Object... args)
  {
    if (!condition) {
      throw exception(t, msg, args);
    }
  }


  public InvalidInput(
      Throwable t,
      String msg,
      Object... args
  )
  {
    super(
        "invalidInput",
        DruidException.Persona.USER,
        DruidException.Category.INVALID_INPUT,
        t, msg, args
    );
  }

}
