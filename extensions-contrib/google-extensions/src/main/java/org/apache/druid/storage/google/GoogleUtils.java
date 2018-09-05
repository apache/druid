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

import java.io.IOException;

public class GoogleUtils
{
  public static String toFilename(String path)
  {
    String filename = path.substring(path.lastIndexOf("/") + 1); // characters after last '/'
    filename = filename.substring(0, filename.length());
    return filename;
  }

  public static String indexZipForSegmentPath(String path)
  {
    return path.substring(0, path.lastIndexOf("/")) + "/index.zip";
  }

  public static boolean isRetryable(Throwable t)
  {
    if (t instanceof HttpResponseException) {
      final HttpResponseException e = (HttpResponseException) t;
      return e.getStatusCode() == 429 || (e.getStatusCode() / 500 == 1);
    }
    return t instanceof IOException;
  }
}
