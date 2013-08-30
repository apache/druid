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

import java.util.List;

/**
 * An InputRow is the interface definition of an event being input into the data ingestion layer.
 *
 * An InputRow is a Row with a self-describing list of the dimensions available.  This list is used to
 * implement "schema-less" data ingestion that allows the system to add new dimensions as they appear.
 *
 * Note, Druid is a case-insensitive system for parts of schema (column names), this has direct implications
 * for the implementation of InputRows and Rows.  The case-insensitiveness is implemented by lowercasing all
 * schema elements before looking them up, this means that calls to getDimension() and getFloatMetric() will
 * have all lowercase column names passed in no matter what is returned from getDimensions or passed in as the
 * fieldName of an AggregatorFactory.  Implementations of InputRow and Row should expect to get values back
 * in all lowercase form (i.e. they should either have already turned everything into lowercase or they
 * should operate in a case-insensitive manner).
 */
public interface InputRow extends Row
{
  /**
   * Returns the dimensions that exist in this row.
   *
   * @return the dimensions that exist in this row.
   */
  public List<String> getDimensions();
}
