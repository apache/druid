/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * A Row of data.  This can be used for both input and output into various parts of the system.  It assumes
 * that the user already knows the schema of the row and can query for the parts that they care about.
 *
 * Note, Druid is a case-insensitive system for parts of schema (column names), this has direct implications
 * for the implementation of InputRows and Rows.  The case-insensitiveness is implemented by lowercasing all
 * schema elements before looking them up, this means that calls to getDimension() and getFloatMetric() will
 * have all lowercase column names passed in no matter what is returned from getDimensions or passed in as the
 * fieldName of an AggregatorFactory.  Implementations of InputRow and Row should expect to get values back
 * in all lowercase form (i.e. they should either have already turned everything into lowercase or they
 * should operate in a case-insensitive manner).
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "version")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "v1", value = MapBasedRow.class)
})
public interface Row
{
  /**
   * Returns the timestamp from the epoch in milliseconds.  If the event happened _right now_, this would return the
   * same thing as System.currentTimeMillis();
   *
   * @return the timestamp from the epoch in milliseconds.
   */
  public long getTimestampFromEpoch();

  /**
   * Returns the list of dimension values for the given column name.
   *
   * Column names are always all lowercase in order to support case-insensitive schemas.
   *
   * @param dimension the lowercase column name of the dimension requested
   * @return the list of values for the provided column name
   */
  public List<String> getDimension(String dimension);

  /**
   * Returns the float value of the given metric column.
   *
   * Column names are always all lowercase in order to support case-insensitive schemas.
   *
   * @param metric the lowercase column name of the metric requested
   * @return the float value for the provided column name.
   */
  public float getFloatMetric(String metric);
}
