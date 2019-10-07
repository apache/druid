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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.guice.annotations.ExtensionPoint;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

/**
 * A ServletFilterHolder is a class that holds all of the information required to attach a Filter to a Servlet.
 *
 * This largely exists just to make it possible to add Filters via Guice/DI and shouldn't really exist
 * anywhere that is not initialization code.
 * 
 * Note that some of the druid nodes (router for example) use async servlets and your filter
 * implementation should be able to handle those requests properly.
 */
@ExtensionPoint
public interface ServletFilterHolder
{
  /**
   * Get the Filter object that should be added to the servlet.
   *
   * This method is considered "mutually exclusive" from the getFilterClass method.
   * That is, one of them should return null and the other should return an actual value.
   *
   * @return The Filter object to be added to the servlet
   */
  Filter getFilter();

  /**
   * Get the class of the Filter object that should be added to the servlet.
   *
   * This method is considered "mutually exclusive" from the getFilter method.
   * That is, one of them should return null and the other should return an actual value.
   *
   * @return The class of the Filter object to be added to the servlet
   */
  Class<? extends Filter> getFilterClass();

  /**
   * Get Filter initialization parameters.
   * 
   * @return a map containing all the Filter initialization
   * parameters 
   */
  Map<String, String> getInitParameters();

  /**
   * This method is deprecated, please implement {@link #getPaths()}.
   *
   * The path that this Filter should apply to
   *
   * @return the path that this Filter should apply to
   */
  @Deprecated
  String getPath();

  /**
   * The paths that this Filter should apply to
   *
   * @return the paths that this Filter should apply to
   */
  default String[] getPaths()
  {
    return new String[]{getPath()};
  }

  /**
   * The dispatcher type that this Filter should apply to
   *
   * @return the enumeration of DispatcherTypes that this Filter should apply to
   */
  @Nullable
  EnumSet<DispatcherType> getDispatcherType();
}
