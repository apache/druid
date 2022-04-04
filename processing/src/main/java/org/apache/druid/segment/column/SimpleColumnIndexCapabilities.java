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

package org.apache.druid.segment.column;

import java.util.Objects;

public class SimpleColumnIndexCapabilities implements ColumnIndexCapabilities
{
  private static final SimpleColumnIndexCapabilities DEFAULT = new SimpleColumnIndexCapabilities(true, true);

  /**
   * {@link ColumnIndexCapabilities} to use for constant values (including 'null' values for 'missing' columns)
   */
  public static ColumnIndexCapabilities getConstant()
  {
    return DEFAULT;
  }

  private final boolean invertible;
  private final boolean exact;

  public SimpleColumnIndexCapabilities(boolean invertible, boolean exact)
  {
    this.invertible = invertible;
    this.exact = exact;
  }

  @Override
  public boolean isInvertible()
  {
    return invertible;
  }

  @Override
  public boolean isExact()
  {
    return exact;
  }

  @Override
  public ColumnIndexCapabilities merge(ColumnIndexCapabilities other)
  {
    return new SimpleColumnIndexCapabilities(
        invertible && other.isInvertible(),
        exact && other.isExact()
    );
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
    SimpleColumnIndexCapabilities that = (SimpleColumnIndexCapabilities) o;
    return invertible == that.invertible && exact == that.exact;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(invertible, exact);
  }
}
