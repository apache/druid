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

package io.druid.segment.serde;

import com.google.common.base.Supplier;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.IndexedComplexColumn;
import io.druid.segment.data.GenericIndexed;

/**
*/
public class ComplexColumnPartSupplier implements Supplier<ComplexColumn>
{
  private final GenericIndexed complexType;
  private final String typeName;

  public ComplexColumnPartSupplier(
      final String typeName, final GenericIndexed complexType
  ) {
    this.complexType = complexType;
    this.typeName = typeName;
  }

  @Override
  public ComplexColumn get()
  {
    return new IndexedComplexColumn(typeName, complexType);
  }
}
