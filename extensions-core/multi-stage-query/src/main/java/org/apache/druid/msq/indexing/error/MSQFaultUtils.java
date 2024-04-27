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

package org.apache.druid.msq.indexing.error;

public class MSQFaultUtils
{

  public static final String ERROR_CODE_DELIMITER = ": ";

  /**
   * Generate string message with error code delimited by {@link MSQFaultUtils#ERROR_CODE_DELIMITER}
   */
  public static String generateMessageWithErrorCode(MSQFault msqFault)
  {
    final String message = msqFault.getErrorMessage();

    if (message != null && !message.isEmpty()) {
      return msqFault.getErrorCode() + ERROR_CODE_DELIMITER + message;
    } else {
      return msqFault.getErrorCode();
    }
  }

  /**
   * Gets the error code from the message. If the message is empty or null, {@link UnknownFault#CODE} is returned. This method
   * does not gurantee that the error code we get out of the message is a valid error code.
   */
  public static String getErrorCodeFromMessage(String message)
  {
    if (message == null || message.isEmpty() || !message.contains(ERROR_CODE_DELIMITER)) {
      return UnknownFault.CODE;
    }
    return message.split(ERROR_CODE_DELIMITER, 2)[0];
  }

}
