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
 * This exception class should be used instead of
 * {@link org.apache.druid.java.util.common.ISE} or {@link org.apache.druid.java.util.common.IAE} when processing is
 * to be halted during planning. There is wiring in place to bubble up the error message to the user when wrapped
 * in this exception class.
 */
public class InvalidSqlInput extends InvalidInput
{
  public static DruidException exception(String msg, Object... args)
  {
    return exception(null, msg, args);
  }

  public static DruidException exception(Throwable t, String msg, Object... args)
  {
    return DruidException.fromFailure(new InvalidSqlInput(t, msg, args));
  }

  public InvalidSqlInput(
      Throwable t,
      String msg,
      Object... args
  )
  {
    super(t, msg, args);
  }

  @Override
  public DruidException makeException(DruidException.DruidExceptionBuilder bob)
  {
    final DruidException retVal = super.makeException(bob);
    retVal.withContext("sourceType", "sql");
    return retVal;
  }
}
