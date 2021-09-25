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

package org.apache.druid.data.input;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Used by some {@link InputSourceReader} implementations in order to know what columns will need to be read out
 * of the {@link InputRow} objects they create.
 *
 * This is meant to be useful as an optimization: if we're reading from a columnar data format, then when a column
 * isn't going to be needed, we shouldn't read it.
 *
 * @see InputSource#reader accepts objects of this class
 */
public abstract class ColumnsFilter
{
  /**
   * Accepts all columns.
   */
  public static ColumnsFilter all()
  {
    return new ExclusionBased(Collections.emptySet());
  }

  /**
   * Accepts a specific list of columns.
   */
  public static ColumnsFilter inclusionBased(final Set<String> inclusions)
  {
    return new InclusionBased(inclusions);
  }


  /**
   * Accepts all columns, except those on a specific list.
   */
  public static ColumnsFilter exclusionBased(final Set<String> exclusions)
  {
    return new ExclusionBased(exclusions);
  }

  /**
   * Check if a column should be included or not.
   */
  public abstract boolean apply(String column);

  /**
   * Returns a new filter with a particular column added. The returned filter will return true from {@link #apply}
   * on this column.
   */
  public abstract ColumnsFilter plus(String column);

  public static class InclusionBased extends ColumnsFilter
  {
    private final Set<String> inclusions;

    private InclusionBased(Set<String> inclusions)
    {
      this.inclusions = inclusions;
    }

    @Override
    public boolean apply(String column)
    {
      return inclusions.contains(column);
    }

    @Override
    public ColumnsFilter plus(String column)
    {
      if (inclusions.contains(column)) {
        return this;
      } else {
        final Set<String> copy = new HashSet<>(inclusions);
        copy.add(column);
        return new InclusionBased(copy);
      }
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
      InclusionBased that = (InclusionBased) o;
      return Objects.equals(inclusions, that.inclusions);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(inclusions);
    }

    @Override
    public String toString()
    {
      return "ColumnsFilter.InclusionBased{" +
             "inclusions=" + inclusions +
             '}';
    }
  }

  public static class ExclusionBased extends ColumnsFilter
  {
    private final Set<String> exclusions;

    public ExclusionBased(Set<String> exclusions)
    {
      this.exclusions = exclusions;
    }

    @Override
    public boolean apply(String column)
    {
      return !exclusions.contains(column);
    }

    @Override
    public ColumnsFilter plus(String column)
    {
      if (!exclusions.contains(column)) {
        return this;
      } else {
        final Set<String> copy = new HashSet<>(exclusions);
        copy.remove(column);
        return new ExclusionBased(copy);
      }
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
      ExclusionBased that = (ExclusionBased) o;
      return Objects.equals(exclusions, that.exclusions);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(exclusions);
    }

    @Override
    public String toString()
    {
      return "ColumnsFilter.ExclusionBased{" +
             "exclusions=" + exclusions +
             '}';
    }
  }
}
