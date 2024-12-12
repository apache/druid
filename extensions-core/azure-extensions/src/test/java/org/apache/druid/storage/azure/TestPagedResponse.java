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

package org.apache.druid.storage.azure;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.IterableStream;

import java.util.Collection;

public class TestPagedResponse<T> implements PagedResponse<T>
{
  private final Collection<T> responseItems;

  public TestPagedResponse(Collection<T> responseItems)
  {
    this.responseItems = responseItems;
  }

  @Override
  public int getStatusCode()
  {
    return 0;
  }

  @Override
  public HttpHeaders getHeaders()
  {
    return null;
  }

  @Override
  public HttpRequest getRequest()
  {
    return null;
  }

  @Override
  public IterableStream<T> getElements()
  {
    return IterableStream.of(responseItems);
  }

  @Override
  public String getContinuationToken()
  {
    return null;
  }

  @Override
  public void close()
  {

  }
}
