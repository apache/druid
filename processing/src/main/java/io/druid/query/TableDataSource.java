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
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("table")
public class TableDataSource implements DataSource
{
  @JsonProperty
  private final String name;

  @JsonCreator
  public TableDataSource(@JsonProperty("name") String name)
  {
    this.name = (name == null ? null : name.toLowerCase());
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  public String toString() { return name; }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableDataSource)) {
      return false;
    }

    TableDataSource that = (TableDataSource) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return name.hashCode();
  }
}
