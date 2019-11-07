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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import org.apache.druid.server.security.AuthenticationUtils;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.resource.Resource;

import java.util.List;

class WebConsoleJettyServerInitializer
{
  private static final List<String> UNSECURED_PATHS_FOR_UI = ImmutableList.of(
      "/",
      "/assets/*",
      "/public/*",
      "/unified-console.html",
      "/favicon.png",
      "/console-config.js"
  );

  static void intializeServerForWebConsoleRoot(ServletContextHandler root)
  {
    root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
    root.setWelcomeFiles(new String[]{"unified-console.html"});

    root.setBaseResource(Resource.newClassPathResource("org/apache/druid/console"));

    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS_FOR_UI);
  }
}
