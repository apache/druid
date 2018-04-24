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

package io.druid.storage.google;

import com.google.common.base.Predicate;

public class GoogleUtils
{
  public static final Predicate<Throwable> GOOGLE_RETRY = new Predicate<Throwable>()
  {
    @Override
    public boolean apply(Throwable e)
    {
      return false;
    }
  };

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
}
