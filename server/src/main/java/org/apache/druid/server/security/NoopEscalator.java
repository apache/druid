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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.http.client.HttpClient;

public class NoopEscalator implements Escalator
{
  private static final NoopEscalator INSTANCE = new NoopEscalator();

  @JsonCreator
  public static NoopEscalator getInstance()
  {
    return INSTANCE;
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return baseClient;
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return AllowAllAuthenticator.ALLOW_ALL_RESULT;
  }

  @Override
  public boolean equals(final Object obj)
  {
    //noinspection ObjectEquality
    return obj.getClass() == getClass();
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "NoopEscalator{}";
  }
}
