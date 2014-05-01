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

import javax.annotation.Nullable;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class UnionDataSource implements DataSource
{
  @JsonProperty
  private final List<DataSource> dataSources;

  @JsonCreator
  public UnionDataSource(@JsonProperty("dataSources") List<DataSource> dataSources)
  {
    Preconditions.checkNotNull(dataSources, "datasources cannot be null for uniondatasource");
    for(DataSource ds : dataSources){
      Preconditions.checkArgument(ds instanceof TableDataSource, "Union DataSource only supports TableDatasource");
    }
    this.dataSources = dataSources;
  }

  @Override
  public Iterable<String> getNames()
  {
   return Iterables.concat(Iterables.transform(dataSources, new Function<DataSource, Iterable<String>>()
   {
     @Override
     public Iterable<String> apply(DataSource input)
     {
       return input.getNames();
     }
   }));
  }

  @Override
  public String getMetricName()
  {
    SortedSet<String> str = new TreeSet<>();
    for(DataSource ds : dataSources){
      str.add(ds.getMetricName());
    }
    return str.toString();
  }

  @JsonProperty
  public List<DataSource> getDataSources(){
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
}
