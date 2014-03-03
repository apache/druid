/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.druid.guice.annotations.Self;
import io.druid.query.Query;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class QueryIDProvider
{
  private final String host;
  private final AtomicLong id = new AtomicLong();

  @Inject
  public QueryIDProvider(@Self DruidNode node)
  {
    host = node.getHost();
  }

  public String next(Query query)
  {
    return String.format(
        "%s_%s_%s_%s_%s",
        query.getDataSource(),
        query.getDuration(),
        host,
        new DateTime(),
        id.incrementAndGet()
    );
  }
}
