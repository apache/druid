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

package org.apache.druid.catalog;

import org.apache.druid.catalog.MetadataCatalog.ColumnKind;
import org.apache.druid.catalog.MetadataCatalog.ColumnMetadata;
import org.apache.druid.catalog.MetadataCatalog.InputColumnMetadata;
import org.apache.druid.catalog.MetadataCatalog.MeasureMetadata;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

public abstract class AbstractColumnMetadata implements ColumnMetadata
{
  public static class SimpleColumn extends AbstractColumnMetadata
  {
    public SimpleColumn(String name, String sqlType)
    {
      super(name, sqlType);
    }

    @Override
    public ColumnKind kind()
    {
      return ColumnKind.SIMPLE;
    }
  }

  public static class DimensionColumn extends AbstractColumnMetadata
  {
    public DimensionColumn(String name, String sqlType)
    {
      super(name, sqlType);
    }

    @Override
    public ColumnKind kind()
    {
      return ColumnKind.DIMENSION;
    }
  }

  public static class MeasureColumn extends AbstractColumnMetadata implements MeasureMetadata
  {
    private final String aggFn;

    public MeasureColumn(String name, String sqlType, String aggFn)
    {
      super(name, sqlType);
      this.aggFn = aggFn;
    }

    @Override
    public ColumnKind kind()
    {
      return ColumnKind.MEASURE;
    }

    @Override
    public String aggFn()
    {
      return aggFn;
    }
  }

  public static class InputColumn extends SimpleColumn implements InputColumnMetadata
  {
    public InputColumn(String name, String sqlType)
    {
      super(name, sqlType);
    }

    @Override
    public ColumnKind kind()
    {
      return ColumnKind.INPUT;
    }

    @Override
    public ColumnType druidType()
    {
      return ColumnDefn.VALID_SQL_TYPES.get(StringUtils.toUpperCase(sqlType));
    }
  }

  protected final String name;
  protected final String sqlType;

  public AbstractColumnMetadata(String name, String sqlType)
  {
    this.name = name;
    this.sqlType = sqlType;
  }

  @Override
  public String name()
  {
    return name;
  }

  @Override
  public String sqlType()
  {
    return sqlType;
  }
}
