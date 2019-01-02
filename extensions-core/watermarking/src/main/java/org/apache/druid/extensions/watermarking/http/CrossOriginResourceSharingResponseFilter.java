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

package org.apache.druid.extensions.watermarking.http;

import com.google.common.base.Strings;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

public class CrossOriginResourceSharingResponseFilter implements ContainerResponseFilter
{
  private final String allowOrigin;

  public CrossOriginResourceSharingResponseFilter()
  {
    this("*");
  }

  // todo: stubbed off to allow exposing config
  public CrossOriginResourceSharingResponseFilter(String allowOrigin)
  {
    this.allowOrigin = Strings.isNullOrEmpty(allowOrigin) ? "*" : allowOrigin;
  }

  @Override
  public ContainerResponse filter(
      ContainerRequest request,
      ContainerResponse response
  )
  {
    response.getHttpHeaders().add("Access-Control-Allow-Origin", allowOrigin);
    response.getHttpHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization");
    response.getHttpHeaders().add("Access-Control-Allow-Credentials", "true");
    response.getHttpHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
    return response;
  }
}
