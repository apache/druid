/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.cloudfiles;

import com.google.common.base.Predicate;

import io.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 *
 */
public class CloudFilesUtils
{

  public static final Predicate<Throwable> CLOUDFILESRETRY = new Predicate<Throwable>()
  {
    @Override
    public boolean apply(Throwable e)
    {
      if (e == null) {
        return false;
      } else if (e instanceof IOException) {
        return true;
      } else {
        return apply(e.getCause());
      }
    }
  };

  /**
   * Retries CloudFiles operations that fail due to io-related exceptions.
   */
  public static <T> T retryCloudFilesOperation(Callable<T> f, final int maxTries) throws Exception
  {
    return RetryUtils.retry(f, CLOUDFILESRETRY, maxTries);
  }

  public static String buildCloudFilesPath(String basePath, final String fileName)
  {
    String path = fileName;
    if (!basePath.isEmpty()) {
      int lastSlashIndex = basePath.lastIndexOf("/");
      if (lastSlashIndex != -1) {
        basePath = basePath.substring(0, lastSlashIndex);
      }
      path = basePath + "/" + fileName;
    }
    return path;
  }

}
