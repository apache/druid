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

package org.apache.druid.common.exception;

import java.util.function.Function;

/**
 * Error response transform strategy that does nothing and simply return the same Exception back without any change
 */
public class NoErrorResponseTransformStrategy implements ErrorResponseTransformStrategy
{
  public static final NoErrorResponseTransformStrategy INSTANCE = new NoErrorResponseTransformStrategy();

  @Override
  public Exception transformIfNeeded(SanitizableException exception)
  {
    return (Exception) exception;
  }

  @Override
  public Function<String, String> getErrorMessageTransformFunction()
  {
    return (String errorMessage) -> errorMessage;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    return !(o == null || getClass() != o.getClass());
  }

  @Override
  public int hashCode()
  {
    return NoErrorResponseTransformStrategy.class.hashCode();
  }
}
