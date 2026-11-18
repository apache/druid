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

package org.apache.druid.storage.s3;

import org.apache.druid.query.context.RequestHeaders;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.util.Map;

/** Retains operation headers across SDK async work and retries, and adds them before signing. */
class S3RequestHeaderInterceptor implements ExecutionInterceptor
{
  static final ExecutionAttribute<Map<String, String>> HEADERS = new ExecutionAttribute<>("DruidRequestHeaders");

  @Override
  public void beforeExecution(final Context.BeforeExecution context, final ExecutionAttributes attributes)
  {
    if (attributes.getAttribute(HEADERS) == null) {
      attributes.putAttribute(HEADERS, RequestHeaders.current());
    }
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(final Context.ModifyHttpRequest context, final ExecutionAttributes attributes)
  {
    final Map<String, String> headers = attributes.getAttribute(HEADERS);
    if (headers == null || headers.isEmpty()) {
      return context.httpRequest();
    }
    final SdkHttpRequest.Builder request = context.httpRequest().toBuilder();
    headers.forEach((header, value) -> {
      // SDK request overrides are merged after this interceptor runs.
      final boolean explicit = context.request().overrideConfiguration()
                                      .map(config -> config.headers().keySet().stream().anyMatch(header::equalsIgnoreCase))
                                      .orElse(false);
      if (!explicit && context.httpRequest().firstMatchingHeader(header).isEmpty()) {
        request.putHeader(header, value);
      }
    });
    return request.build();
  }
}
