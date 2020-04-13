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

package org.apache.druid.java.util.http.client.response;

import javax.annotation.Nullable;

/**
 */
public class ClientResponse<T>
{
  private final boolean finished;
  private final boolean continueReading;

  @Nullable
  private final T obj;

  public static <T> ClientResponse<T> finished(T obj)
  {
    return new ClientResponse<>(true, true, obj);
  }

  public static <T> ClientResponse<T> finished(T obj, boolean continueReading)
  {
    return new ClientResponse<>(true, continueReading, obj);
  }

  public static <T> ClientResponse<T> unfinished(T obj)
  {
    return new ClientResponse<>(false, true, obj);
  }

  public static <T> ClientResponse<T> unfinished(T obj, boolean continueReading)
  {
    return new ClientResponse<>(false, continueReading, obj);
  }

  public ClientResponse(final boolean finished, final boolean continueReading, @Nullable final T obj)
  {
    this.finished = finished;
    this.continueReading = continueReading;
    this.obj = obj;
  }

  public boolean isFinished()
  {
    return finished;
  }

  public boolean isContinueReading()
  {
    return continueReading;
  }

  @Nullable
  public T getObj()
  {
    return obj;
  }
}
