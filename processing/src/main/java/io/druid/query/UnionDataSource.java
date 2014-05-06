/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.query;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

public class UnionDataSource implements DataSource
{
  @JsonProperty
  private final List<TableDataSource> dataSources;

  @JsonCreator
  public UnionDataSource(@JsonProperty("dataSources") List<TableDataSource> dataSources)
  {
    Preconditions.checkNotNull(dataSources, "dataSources cannot be null for unionDataSource");
    this.dataSources = dataSources;
  }

  @Override
  public List<String> getNames()
  {
    return Lists.transform(
        dataSources,
        new Function<TableDataSource, String>()
        {
          @Override
          public String apply(TableDataSource input)
          {
            return Iterables.getOnlyElement(input.getNames());
          }
        }
    );
  }

  @JsonProperty
  public List<TableDataSource> getDataSources()
  {
    return dataSources;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UnionDataSource that = (UnionDataSource) o;

    if (!dataSources.equals(that.dataSources)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return dataSources.hashCode();
  }

  @Override
  public String toString()
  {
    return "UnionDataSource{" +
           "dataSources=" + dataSources +
           '}';
  }
}
