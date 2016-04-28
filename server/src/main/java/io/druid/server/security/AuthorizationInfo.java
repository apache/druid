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

package io.druid.server.security;

/**
 * This interface should be used to store as well as process Authorization Information
 * An extension can be used to inject servlet filter which will create objects of this type
 * and set it as a request attribute with attribute name as {@link AuthConfig#DRUID_AUTH_TOKEN}.
 * In the jersey resources if the authorization is enabled depending on {@link AuthConfig#enabled}
 * the {@link #isAuthorized(Resource, Action)} method will be used to perform authorization checks
 * */
public interface AuthorizationInfo
{
  /**
   * Perform authorization checks for the given {@link Resource} and {@link Action}.
   * <code>resource</code> and <code>action</code> objects should be instantiated depending on
   * the specific endPoint where the check is being performed.
   * Modeling Principal and specific way of performing authorization checks is
   * entirely implementation dependent.
   *
   * @param resource information about resource that is being accessed
   * @param action action to be performed on the resource
   * @return a {@link Access} object having {@link Access#allowed} set to true if authorized otherwise set to false
   *          and optionally {@link Access#message} set to appropriate message
   * */
  Access isAuthorized(Resource resource, Action action);
}
