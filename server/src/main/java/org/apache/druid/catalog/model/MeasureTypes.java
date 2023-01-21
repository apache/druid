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

package org.apache.druid.catalog.model;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MeasureTypes
{
  public static final String OPTIONAL = "--";

  public enum BaseType
  {
    VARCHAR(ColumnType.STRING),
    BIGINT(ColumnType.LONG),
    FLOAT(ColumnType.FLOAT),
    DOUBLE(ColumnType.DOUBLE);

    public final ColumnType nativeType;

    BaseType(ColumnType nativeType)
    {
      this.nativeType = nativeType;
    }
  }

  public static class MeasureType
  {
    public final String name;
    public final List<BaseType> argTypes;
    public final String sqlSeedFn;
    public final String sqlReducerFn;
    public final String nativeType;
    public final String nativeAggFn;
    public final ColumnType storageType;
    public final String nativeReducerFn;

    public MeasureType(
        final String name,
        final List<BaseType> argTypes,
        final String sqlSeedFn,
        final String sqlReducerFn,
        final String nativeType,
        final ColumnType storageType,
        final String nativeAggFn,
        final String nativeReducerFn
    )
    {
      this.name = name;
      this.argTypes = argTypes == null ? Collections.emptyList() : argTypes;
      this.sqlSeedFn = sqlSeedFn;
      this.sqlReducerFn = sqlReducerFn;
      this.nativeType = nativeType;
      this.storageType = storageType;
      this.nativeAggFn = nativeAggFn;
      this.nativeReducerFn = nativeReducerFn;
    }

    @Override
    public String toString()
    {
      StringBuilder buf = new StringBuilder()
          .append(name)
          .append("(");
      for (int i = 0; i < argTypes.size(); i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append(argTypes.get(i).name());
      }
      return buf.append(")").toString();
    }
  }

  // See: https://druid.apache.org/docs/latest/querying/aggregations.html
  public static final MeasureType COUNT_TYPE = new MeasureType(
      "COUNT",
      null,
      null,
      "SUM",
      "longSum",
      ColumnType.LONG,
      "count",
      "longSum"
  );

  public static final MeasureType SUM_BIGINT_TYPE = simpleAggType("sum", BaseType.BIGINT);
  public static final MeasureType SUM_FLOAT_TYPE = simpleAggType("sum", BaseType.FLOAT);
  public static final MeasureType SUM_DOUBLE_TYPE = simpleAggType("sum", BaseType.DOUBLE);
  public static final MeasureType MIN_BIGINT_TYPE = simpleAggType("min", BaseType.BIGINT);
  public static final MeasureType MIN_FLOAT_TYPE = simpleAggType("min", BaseType.FLOAT);
  public static final MeasureType MIN_DOUBLE_TYPE = simpleAggType("min", BaseType.DOUBLE);
  public static final MeasureType MAX_BIGINT_TYPE = simpleAggType("max", BaseType.BIGINT);
  public static final MeasureType MAX_FLOAT_TYPE = simpleAggType("max", BaseType.FLOAT);
  public static final MeasureType MAX_DOUBLE_TYPE = simpleAggType("max", BaseType.DOUBLE);

  private static MeasureType simpleAggType(String fn, BaseType baseType)
  {
    String sqlFn = StringUtils.toUpperCase(fn);
    String nativeFn = baseType.nativeType.asTypeString() + org.apache.commons.lang3.StringUtils.capitalize(fn);
    return new MeasureType(
        sqlFn,
        Collections.singletonList(baseType),
        sqlFn,
        null,
        sqlFn,
        baseType.nativeType,
        nativeFn,
        nativeFn
    );
  }

  private static final List<MeasureType> TYPE_LIST =
      Arrays.asList(
          COUNT_TYPE,
          SUM_BIGINT_TYPE,
          SUM_FLOAT_TYPE,
          SUM_DOUBLE_TYPE,
          MIN_BIGINT_TYPE,
          MIN_FLOAT_TYPE,
          MIN_DOUBLE_TYPE,
          MAX_BIGINT_TYPE,
          MAX_FLOAT_TYPE,
          MAX_DOUBLE_TYPE
      );
  public static final Map<String, List<MeasureType>> TYPES;

  static {
    Map<String, List<MeasureType>> map = new HashMap<>();
    for (MeasureType fn : TYPE_LIST) {
      List<MeasureType> overloads = map.computeIfAbsent(fn.name, x -> new ArrayList<>());
      overloads.add(fn);
    }
    TYPES = ImmutableMap.<String, List<MeasureType>>builder().putAll(map).build();
  }

  public static MeasureType parse(String typeStr)
  {
    Pattern p = Pattern.compile("(\\w+)(?:\\s*\\((.*)\\))?");
    Matcher m = p.matcher(StringUtils.toUpperCase(typeStr.trim()));
    if (!m.matches()) {
      throw new IAE(StringUtils.format(
          "The type [%s] is not well-formed. It must be FN, FN(TYPE) or FN(TYPE,TYPE...)",
          typeStr
      ));
    }
    String fnName = m.group(1);
    String[] args;
    String argGroup = m.group(2);
    argGroup = argGroup == null ? null : argGroup.trim();
    if (Strings.isNullOrEmpty(argGroup)) {
      args = new String[] {};
    } else {
      args = argGroup.trim().split("\\s*,\\s*");
    }
    return parse(typeStr, fnName, Arrays.asList(args));
  }

  public static MeasureType parse(String typeStr, String fnName, List<String> args)
  {
    List<MeasureType> candidates = TYPES.get(fnName);
    if (candidates == null) {
      throw new IAE(StringUtils.format(
          "The metric type [%s] is not valid.",
          fnName
      ));
    }

    top:
    for (MeasureType type : candidates) {
      if (type.argTypes.size() != args.size()) {
        continue;
      }
      for (int i = 0; i < args.size(); i++) {
        if (!type.argTypes.get(i).name().equalsIgnoreCase(args.get(i))) {
          continue top;
        }
      }
      return type;
    }

    throw new IAE(StringUtils.format(
          "[%s] is not a valid metric type. Valid forms are %s",
          typeStr,
          candidates.stream().map(t -> t.toString()).collect(Collectors.joining(", "))
          )
    );
  }
}
