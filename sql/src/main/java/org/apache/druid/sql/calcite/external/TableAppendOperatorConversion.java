///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.druid.sql.calcite.external;
//
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.sql.SqlOperator;
//import org.apache.druid.segment.column.RowSignature;
//import org.apache.druid.sql.calcite.expression.DruidExpression;
//import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
//import org.apache.druid.sql.calcite.planner.PlannerContext;
//
///**
// * FIXME
// *
// * alternate names: * TABLE_CONCAT select * from TABLE(TABLE_CONCAT('t1','t2')) * APPEND select * from
// * TABLE(APPEND('t1','t2')) * CONCAT select * from TABLE(CONCAT('t1','t2'))
// */
//public class TableAppendOperatorConversion implements SqlOperatorConversion
//{
//  public static final String FUNCTION_NAME = "APPEND";
//  public static final TableAppendOperatorConversion INSTANCE = new TableAppendOperatorConversion();
//  private AppendTableMacro macro;
//
//  public TableAppendOperatorConversion()
//  {
//    macro = new AppendTableMacro();
//  }
//
//  @Override
//  public SqlOperator calciteOperator()
//  {
//    return macro;
//  }
//
//  @Override
//  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
//  {
//    throw new IllegalStateException();
//  }
//}
