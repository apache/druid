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

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.sql.Date;
import java.util.Arrays;

public class ExpressionConverterTest
{
  @Test
  public void test()
  {
    ExprNodeColumnDesc longTime = new ExprNodeColumnDesc(Long.class, "__time", "some_table", false);
    ExprNodeColumnDesc dateTime = new ExprNodeColumnDesc(Date.class, "__time", "some_table", false);

    ExprNodeColumnDesc someColumn1 = new ExprNodeColumnDesc(String.class, "col1", "some_table", false);
    ExprNodeColumnDesc someColumn2 = new ExprNodeColumnDesc(String.class, "col2", "some_table", false);

    GenericUDFBridge timeAsDouble = new GenericUDFBridge("double", false, UDFToDouble.class.getName());
    ExprNodeGenericFuncDesc gt = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPGreaterThan(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeGenericFuncDesc(PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                                        timeAsDouble, Arrays.<ExprNodeDesc>asList(longTime)),
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 1031555555123L)
        )
    );
    ExprNodeGenericFuncDesc lt = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPLessThan(),
        Arrays.<ExprNodeDesc>asList(
            longTime,
            new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 1231555555123L)
        )
    );
    ExprNodeGenericFuncDesc noise1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPLessThan(),
        Arrays.<ExprNodeDesc>asList(
            someColumn1,
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value1")
        )
    );
    ExprNodeGenericFuncDesc noise2 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPLessThan(),
        Arrays.<ExprNodeDesc>asList(
            someColumn2,
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "value2")
        )
    );
    ExprNodeGenericFuncDesc range = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(lt, gt)
    );

    ExprNodeGenericFuncDesc between = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFBetween(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeConstantDesc(false),
            dateTime,
            new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, new Date(105, 3, 12)),
            new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, new Date(115, 3, 14))
        )
    );

    ExprNodeGenericFuncDesc complex1 = new ExprNodeGenericFuncDesc(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        new GenericUDFOPAnd(),
        Arrays.<ExprNodeDesc>asList(
            new ExprNodeGenericFuncDesc(
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
                new GenericUDFOPAnd(),
                Arrays.<ExprNodeDesc>asList(noise1, lt)
            ),
            between,
            new ExprNodeGenericFuncDesc(
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
                new GenericUDFOPAnd(),
                Arrays.<ExprNodeDesc>asList(gt, noise2)
            )
        )
    );


    System.out.println("[ExpressionConverterTest/test] " + ExpressionConverter.getIntervals(gt));
    System.out.println("[ExpressionConverterTest/test] " + ExpressionConverter.getIntervals(lt));
    System.out.println("[ExpressionConverterTest/test] " + ExpressionConverter.getIntervals(range));
    System.out.println("[ExpressionConverterTest/test] " + ExpressionConverter.getIntervals(between));

    System.out.println("[ExpressionConverterTest/test] " + ExpressionConverter.getIntervals(complex1));
  }
}