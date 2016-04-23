/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.filter.DimFilter;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExpressionConverterTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void test() throws Exception
  {
    Map<String, PrimitiveTypeInfo> types = Maps.newHashMap();
    types.put("__time", TypeInfoFactory.longTypeInfo);
    types.put("col1", TypeInfoFactory.stringTypeInfo);
    types.put("col2", TypeInfoFactory.stringTypeInfo);

    ExprNodeColumnDesc longTime = new ExprNodeColumnDesc(Long.class, "__time", "some_table", false);
    ExprNodeColumnDesc timestampTime = new ExprNodeColumnDesc(Timestamp.class, "__time", "some_table", false);

    ExprNodeColumnDesc someColumn1 = new ExprNodeColumnDesc(String.class, "col1", "some_table", false);
    ExprNodeColumnDesc someColumn2 = new ExprNodeColumnDesc(String.class, "col2", "some_table", false);

    // cannot do this
//    ExprNodeGenericFuncDesc timeCastToDouble = new ExprNodeGenericFuncDesc(
//        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
//        new GenericUDFBridge("double", false, UDFToDouble.class.getName()), Arrays.<ExprNodeDesc>asList(longTime)
//    );

    ExprNodeGenericFuncDesc noise1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPNotEqual(),
        Arrays.<ExprNodeDesc>asList(
            someColumn1,
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value1")
        )
    );
    ExprNodeGenericFuncDesc noise2 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPGreaterThan(),
        Arrays.<ExprNodeDesc>asList(
            someColumn2,
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value2")
        )
    );
    ExprNodeGenericFuncDesc noise3 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFIn(),
        Arrays.<ExprNodeDesc>asList(
            someColumn2,
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value1"),
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value2")
        )
    );
    ExprNodeGenericFuncDesc gt = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPGreaterThan(),
        Arrays.<ExprNodeDesc>asList(
            timestampTime,
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new Timestamp(new DateTime(2010, 1, 1, 0, 0).getMillis()))
        )
    );
    ExprNodeGenericFuncDesc lt = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPLessThan(),
        Arrays.<ExprNodeDesc>asList(
            longTime,
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new DateTime(2012, 3, 1, 0, 0).getMillis())
        )
    );
    ExprNodeGenericFuncDesc all = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPOr(),
        Arrays.<ExprNodeDesc>asList(lt, gt)
    );
    ExprNodeGenericFuncDesc mixed1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(noise3, lt)
    );
    ExprNodeGenericFuncDesc between0 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(lt, gt)
    );

    ExprNodeGenericFuncDesc between1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFBetween(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeConstantDesc(false),
            longTime,
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new DateTime(2011, 6, 1, 10, 0).getMillis()),
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new DateTime(2016, 4, 1, 12, 0).getMillis())
        )
    );
    ExprNodeGenericFuncDesc between2 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFBetween(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeConstantDesc(false),
            longTime,
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new DateTime(2016, 4, 1, 12, 0).getMillis()),
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, new DateTime(2017, 1, 1, 12, 10).getMillis())
        )
    );

    ExprNodeGenericFuncDesc intersectAnd = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(between0, between1)
    );
    ExprNodeGenericFuncDesc intersectOr = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPOr(),
        Arrays.<ExprNodeDesc>asList(between0, between1)
    );

    ExprNodeGenericFuncDesc abutAnd = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(between1, between2)
    );
    ExprNodeGenericFuncDesc abutOr = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPOr(),
        Arrays.<ExprNodeDesc>asList(between1, between2)
    );

    ExprNodeGenericFuncDesc split = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPOr(),
        Arrays.<ExprNodeDesc>asList(between0, between2)
    );

    ExprNodeGenericFuncDesc complex1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPOr(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeGenericFuncDesc(
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
                new GenericUDFOPAnd(),
                Arrays.<ExprNodeDesc>asList(noise1, lt)
            ),
            new ExprNodeGenericFuncDesc(
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
                new GenericUDFOPAnd(),
                Arrays.<ExprNodeDesc>asList(
                    between1,
                    new ExprNodeGenericFuncDesc(
                        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
                        new GenericUDFOPAnd(),
                        Arrays.<ExprNodeDesc>asList(noise2, gt)
                    )
                )
            )
        )
    );

    validate(
        all,
        types,
        Arrays.asList("(-∞‥+∞)"),
        Arrays.asList("-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")
    );
    validate(
        mixed1,
        types,
        Arrays.asList("(-∞‥1330560000000)"),
        Arrays.asList("-146136543-09-08T08:23:32.096Z/2012-03-01T00:00:00.000Z")
    );
    validate(
        between0,
        types,
        Arrays.asList("(1262304000000‥1330560000000)"),
        Arrays.asList("2010-01-01T00:00:00.001Z/2012-03-01T00:00:00.000Z")
    );
    validate(
        between1,
        types,
        Arrays.asList("[1306922400000‥1459512000000]"),
        Arrays.asList("2011-06-01T10:00:00.000Z/2016-04-01T12:00:00.001Z")
    );
    validate(
        between2,
        types,
        Arrays.asList("[1459512000000‥1483272600000]"),
        Arrays.asList("2016-04-01T12:00:00.000Z/2017-01-01T12:10:00.001Z")
    );

    // 0 AND 1
    validate(
        intersectAnd,
        types,
        Arrays.asList("[1306922400000‥1330560000000)"),
        Arrays.asList("2011-06-01T10:00:00.000Z/2012-03-01T00:00:00.000Z")
    );
    // 0 OR 1
    validate(
        intersectOr,
        types,
        Arrays.asList("(1262304000000‥1459512000000]"),
        Arrays.asList("2010-01-01T00:00:00.001Z/2016-04-01T12:00:00.001Z")
    );
    // 1 AND 2
    validate(
        abutAnd,
        types,
        Arrays.asList("[1459512000000‥1459512000000]"),
        Arrays.asList("2016-04-01T12:00:00.000Z/2016-04-01T12:00:00.001Z")
    );
    // 1 OR 2
    validate(
        abutOr,
        types,
        Arrays.asList("[1306922400000‥1483272600000]"),
        Arrays.asList("2011-06-01T10:00:00.000Z/2017-01-01T12:10:00.001Z")
    );
    // 0 OR 2
    validate(
        split,
        types,
        Arrays.asList("(1262304000000‥1330560000000)", "[1459512000000‥1483272600000]"),
        Arrays.asList(
            "2010-01-01T00:00:00.001Z/2012-03-01T00:00:00.000Z",
            "2016-04-01T12:00:00.000Z/2017-01-01T12:10:00.001Z"
        )
    );

    validate(
        complex1,
        types,
        Arrays.asList("(-∞‥1459512000000]"),
        Arrays.asList("-146136543-09-08T08:23:32.096Z/2016-04-01T12:00:00.001Z")
    );
  }

  private void validate(
      ExprNodeGenericFuncDesc predicate,
      Map<String, PrimitiveTypeInfo> types,
      List<String> expected1,
      List<String> expected2
  ) throws Exception
  {
    Map<String, List<Range>> converted = ExpressionConverter.getRanges(predicate, types);
    List<Range> ranges = converted.remove(ExpressionConverter.TIME_COLUMN_NAME);
    List<Interval> intervals = ExpressionConverter.toInterval(ranges);
    Assert.assertEquals(expected1, Lists.transform(ranges, Functions.toStringFunction()));
    Assert.assertEquals(expected2, Lists.transform(intervals, Functions.toStringFunction()));
    for (Map.Entry<String, List<Range>> entry : converted.entrySet()) {
      System.out.println(
          mapper.readValue(
              mapper.writeValueAsString(ExpressionConverter.toFilter(entry.getKey(), entry.getValue())), DimFilter.class
          )
      );
    }
  }
}
