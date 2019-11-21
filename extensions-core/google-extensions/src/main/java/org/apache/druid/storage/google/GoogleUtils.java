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

package org.apache.druid.storage.google;

import com.google.api.client.http.HttpResponseException;
import com.google.common.base.Predicate;

import java.io.IOException;
import java.net.URI;

public class GoogleUtils
{
  public static boolean isRetryable(Throwable t)
  {
    if (t instanceof HttpResponseException) {
      final HttpResponseException e = (HttpResponseException) t;
      return e.getStatusCode() == 429 || (e.getStatusCode() / 500 == 1);
    }
    return t instanceof IOException;
  }

  public static String extractGoogleCloudStorageObjectKey(URI uri)
  {
    return uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
  }

  public static final Predicate<Throwable> GOOGLE_RETRY = e -> isRetryable(e);
}
