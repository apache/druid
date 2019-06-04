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

package org.apache.druid.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Methods in this class could have belonged to {@link org.apache.druid.java.util.common.io.Closer}, but not editing
 * that class to keep its source close to Guava source.
 */
public final class CloseableUtils
{
  /**
   * Call method instead of code like
   *
   * first.close();
   * second.close();
   *
   * to have safety of {@link org.apache.druid.java.util.common.io.Closer}, but without associated boilerplate code
   * of creating a Closer and registering objects in it.
   */
  public static void closeBoth(Closeable first, Closeable second) throws IOException
  {
    //noinspection EmptyTryBlock
    try (Closeable ignore1 = second;
         Closeable ignore2 = first) {
      // piggy-back try-with-resources semantics
    }
  }

  private CloseableUtils()
  {
  }
}
