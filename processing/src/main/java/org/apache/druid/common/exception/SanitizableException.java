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

public interface SanitizableException
{
  /**
   * Apply the function for transforming the error message then return new Exception with sanitized fields and transformed message.
   * The {@param errorMessageTransformFunction} is only intended to be use to transform the error message
   * String of the Exception as only the error message String is common to all Exception classes.
   * For other fields (which may be unique to each particular Exception class), each implementation of this method can
   * decide for itself how to sanitized those fields (i.e. leaving unchanged, changing to null, changing to a fixed String, etc.).
   * Note that this method returns a new Exception of the same type since Exception error message is immutable.
   */
  Exception sanitize(
      Function<String, String> errorMessageTransformFunction
  );
}
