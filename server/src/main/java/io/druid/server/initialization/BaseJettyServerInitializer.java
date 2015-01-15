/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
