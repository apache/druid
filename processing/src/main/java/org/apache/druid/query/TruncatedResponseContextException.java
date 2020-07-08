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

package org.apache.druid.query;

import org.apache.druid.java.util.common.StringUtils;

/**
 * This exception is thrown when {@link org.apache.druid.query.context.ResponseContext} is truncated after serialization
 * in historicals or realtime tasks. The serialized response context can be truncated if its size is larger than
 * {@code QueryResource#RESPONSE_CTX_HEADER_LEN_LIMIT}.
 *
 * See {@link org.apache.druid.query.context.ResponseContext#serializeWith} and
 * {@code ResponseContextConfig#shouldFailOnTruncatedResponseContext}.
 */
public class TruncatedResponseContextException extends RuntimeException
{
  public TruncatedResponseContextException(String message, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(message, arguments));
  }
}
