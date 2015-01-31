/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.initialization;

import com.google.common.base.Joiner;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlets.AsyncGzipFilter;
import org.eclipse.jetty.servlets.GzipFilter;

import javax.ws.rs.HttpMethod;

public abstract class BaseJettyServerInitializer implements JettyServerInitializer
{

  public static final String GZIP_METHODS = Joiner.on(",").join(HttpMethod.GET, HttpMethod.POST);

  public FilterHolder defaultGzipFilterHolder()
  {
    final FilterHolder gzipFilterHolder = new FilterHolder(GzipFilter.class);
    setDefaultGzipFilterHolderParameters(gzipFilterHolder);
    return gzipFilterHolder;
  }

  public FilterHolder defaultAsyncGzipFilterHolder()
  {
    final FilterHolder gzipFilterHolder = new FilterHolder(AsyncGzipFilter.class);
    setDefaultGzipFilterHolderParameters(gzipFilterHolder);
    return gzipFilterHolder;
  }

  private static void setDefaultGzipFilterHolderParameters(final FilterHolder filterHolder)
  {
    filterHolder.setInitParameter("minGzipSize", "0");
    filterHolder.setInitParameter("methods", GZIP_METHODS);

    // We don't actually have any precomputed .gz resources, and checking for them inside jars is expensive.
    filterHolder.setInitParameter("checkGzExists", String.valueOf(false));
  }
}
