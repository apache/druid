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

import javax.annotation.Nullable;

public final class Throwables
{
  /**
   * searches for a throwable in the cause chain that is of same type as {@param searchFor}. The class of returned
   * throwable will either be same as {@param searchFor} or a sub-class of {@param searchFor}.
   * @param t - the throwable in which to search
   * @param searchFor - Class of throwable to search for
   * @return null if not found otherwise the cause exception that is of same type as searchFor
   */
  @Nullable
  public static <T extends Throwable> T getCauseOfType(Throwable t, Class<T> searchFor)
  {
    if (searchFor.isAssignableFrom(t.getClass())) {
      return (T) t;
    } else {
      if (t.getCause() != null) {
        return getCauseOfType(t.getCause(), searchFor);
      } else {
        return null;
      }
    }
  }

  private Throwables()
  {
  }
}
