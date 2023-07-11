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

 // TODO jvs 15-Nov-2003:  SQL standard allows parentheses in the FROM list for
 // building up non-linear join trees (e.g. OUTER JOIN two tables, and then INNER
 // JOIN the result).  Also note that aliases on parenthesized FROM expressions
 // "hide" all table names inside the parentheses (without aliases, they're
 // visible).
 //
 // We allow CROSS JOIN to have a join condition, even though that is not valid
 // SQL; the validator will catch it.

 // DRUID NOTE : This is an implementation of the FROM clause from the Parser.jj file in Calcite as of version 1.21.0.
 // This file also prefixes the required production rules with 'Druid' so that the whole FROM production rule can be
 // derived from this file itself. The production clause is injected in the grammar using the maven replace plugin in 
 // sql module's pom.

 /**
  * Parses the FROM clause for a SELECT.
  *
  * <p>FROM is mandatory in standard SQL, optional in dialects such as MySQL,
  * PostgreSQL. The parser allows SELECT without FROM, but the validator fails
  * if conformance is, say, STRICT_2003.
  */
 SqlNode DruidFromClause() :
 {
     SqlNode e, e2, condition;
     SqlLiteral natural, joinType, joinConditionType;
     SqlNodeList list;
     SqlParserPos pos;
 }
 {
     e = DruidTableRef()
     (
         LOOKAHEAD(2)
         (
             // Decide whether to read a JOIN clause or a comma, or to quit having
             // seen a single entry FROM clause like 'FROM emps'. See comments
             // elsewhere regarding <COMMA> lookahead.
             //
             // And LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2). Because currently JavaCC
             // calculates minimum lookahead count incorrectly for choice that contains zero size
             // child. For instance, with the generated code, "LOOKAHEAD(2, Natural(), JoinType())"
             // returns true immediately if it sees a single "<CROSS>" token. Where we expect
             // the lookahead succeeds after "<CROSS> <APPLY>".
             //
             // For more information about the issue, see https://github.com/javacc/javacc/issues/86
             LOOKAHEAD(3)
             natural = Natural()
             joinType = JoinType()
             e2 = DruidTableRef()
             (
                 <ON> {
                     joinConditionType = JoinConditionType.ON.symbol(getPos());
                 }
                 condition = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                     e = new SqlJoin(joinType.getParserPosition(),
                         e,
                         natural,
                         joinType,
                         e2,
                         joinConditionType,
                         condition);
                 }
             |
                 <USING> {
                     joinConditionType = JoinConditionType.USING.symbol(getPos());
                 }
                 list = ParenthesizedSimpleIdentifierList() {
                     e = new SqlJoin(joinType.getParserPosition(),
                         e,
                         natural,
                         joinType,
                         e2,
                         joinConditionType,
                         new SqlNodeList(list.getList(), Span.of(joinConditionType).end(this)));
                 }
             |
                 {
                     e = new SqlJoin(joinType.getParserPosition(),
                         e,
                         natural,
                         joinType,
                         e2,
                         JoinConditionType.NONE.symbol(joinType.getParserPosition()),
                         null);
                 }
             )
         |
             // NOTE jvs 6-Feb-2004:  See comments at top of file for why
             // hint is necessary here.  I had to use this special semantic
             // lookahead form to get JavaCC to shut up, which makes
             // me even more uneasy.
             //LOOKAHEAD({true})
             <COMMA> { joinType = JoinType.COMMA.symbol(getPos()); }
             e2 = DruidTableRef() {
                 e = new SqlJoin(joinType.getParserPosition(),
                     e,
                     SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                     joinType,
                     e2,
                     JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                     null);
             }
         |
             <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
             e2 = DruidTableRef2(true) {
                 if (!this.conformance.isApplyAllowed()) {
                     throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                 }
                 e = new SqlJoin(joinType.getParserPosition(),
                     e,
                     SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                     joinType,
                     e2,
                     JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                     null);
             }
         |
             <OUTER> { joinType = JoinType.LEFT.symbol(getPos()); } <APPLY>
             e2 = DruidTableRef2(true) {
                 if (!this.conformance.isApplyAllowed()) {
                     throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                 }
                 e = new SqlJoin(joinType.getParserPosition(),
                     e,
                     SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                     joinType,
                     e2,
                     JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                     SqlLiteral.createBoolean(true, joinType.getParserPosition()));
             }
         )
     )*
     {
         return e;
     }
 }

 /**
  * Parses a table reference in a FROM clause, not lateral unless LATERAL
  * is explicitly specified.
  */
 SqlNode DruidTableRef() :
 {
     final SqlNode e;
 }
 {
     e = DruidTableRef2(false) { return e; }
 }

 /**
  * Parses a table reference in a FROM clause.
  */
 SqlNode DruidTableRef2(boolean lateral) :
 {
     List<SqlNode> paramList;
     SqlNode tableRef;
     final SqlNode over;
     final SqlNode snapshot;
     final SqlNode match;
     SqlNodeList extendList = null;
     final SqlIdentifier alias;
     final Span s, s2;
     SqlNodeList args;
     SqlNode sample;
     boolean isBernoulli;
     SqlNumericLiteral samplePercentage;
     boolean isRepeatable = false;
     int repeatableSeed = 0;
     SqlNodeList columnAliasList = null;
     SqlUnnestOperator unnestOp = SqlStdOperatorTable.UNNEST;
 }
 {
     (
         LOOKAHEAD(2)
         tableRef = CompoundIdentifier()
         [
             paramList = FunctionParameterList(ExprContext.ACCEPT_NONCURSOR)
             {
                 tableRef = ParameterizeOperator.PARAM.createCall(tableRef, paramList);
             }
         ]
         over = TableOverOpt() {
             if (over != null) {
                 tableRef = SqlStdOperatorTable.OVER.createCall(
                     getPos(), tableRef, over);
             }
         }
         [
             snapshot = Snapshot(tableRef) {
                 tableRef = SqlStdOperatorTable.LATERAL.createCall(
                     getPos(), snapshot);
             }
         ]
         [
             tableRef = MatchRecognize(tableRef)
         ]
     |
         LOOKAHEAD(2)
         [ <LATERAL> { lateral = true; } ]
         tableRef = ParenthesizedExpression(ExprContext.ACCEPT_QUERY)
         over = TableOverOpt()
         {
             if (over != null) {
                 tableRef = SqlStdOperatorTable.OVER.createCall(
                     getPos(), tableRef, over);
             }
             if (lateral) {
                 tableRef = SqlStdOperatorTable.LATERAL.createCall(
                     getPos(), tableRef);
             }
         }
         [
             tableRef = MatchRecognize(tableRef)
         ]
     |
         <UNNEST> { s = span(); }
         args = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
         [
             <WITH> <ORDINALITY> {
                 unnestOp = SqlStdOperatorTable.UNNEST_WITH_ORDINALITY;
             }
         ]
         {
             tableRef = unnestOp.createCall(s.end(this), args.toArray());
         }
     |
         [<LATERAL> { lateral = true; } ]
         <TABLE> { s = span(); } <LPAREN>
         tableRef = TableFunctionCall(s.pos())
         <RPAREN>
         [
             [ <EXTEND> ]
             extendList = ExtendList()
             {
                 tableRef = ExtendOperator.EXTEND.createCall(
                         Span.of(tableRef, extendList).pos(), tableRef, extendList);
             }
         ]
         {
             if (lateral) {
                 tableRef = SqlStdOperatorTable.LATERAL.createCall(
                     s.end(this), tableRef);
             }
         }
     |
         tableRef = ExtendedTableRef()
     )
     [
         [ <AS> ] alias = SimpleIdentifier()
         [ columnAliasList = ParenthesizedSimpleIdentifierList() ]
         {
             if (columnAliasList == null) {
                 tableRef = SqlStdOperatorTable.AS.createCall(
                     Span.of(tableRef).end(this), tableRef, alias);
             } else {
                 List<SqlNode> idList = new ArrayList<SqlNode>();
                 idList.add(tableRef);
                 idList.add(alias);
                 idList.addAll(columnAliasList.getList());
                 tableRef = SqlStdOperatorTable.AS.createCall(
                     Span.of(tableRef).end(this), idList);
             }
         }
     ]
     [
         <TABLESAMPLE> { s2 = span(); }
         (
             <SUBSTITUTE> <LPAREN> sample = StringLiteral() <RPAREN>
             {
                 String sampleName =
                     SqlLiteral.unchain(sample).getValueAs(String.class);
                 SqlSampleSpec sampleSpec = SqlSampleSpec.createNamed(sampleName);
                 final SqlLiteral sampleLiteral =
                     SqlLiteral.createSample(sampleSpec, s2.end(this));
                 tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                     s2.add(tableRef).end(this), tableRef, sampleLiteral);
             }
         |
             (
                 <BERNOULLI>
                 {
                     isBernoulli = true;
                 }
             |
                 <SYSTEM>
                 {
                     isBernoulli = false;
                 }
             )
             <LPAREN> samplePercentage = UnsignedNumericLiteral() <RPAREN>
             [
                 <REPEATABLE> <LPAREN> repeatableSeed = IntLiteral() <RPAREN>
                 {
                     isRepeatable = true;
                 }
             ]
             {
                 final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100L);
                 BigDecimal rate = samplePercentage.bigDecimalValue();
                 if (rate.compareTo(BigDecimal.ZERO) < 0
                     || rate.compareTo(ONE_HUNDRED) > 0)
                 {
                     throw SqlUtil.newContextException(getPos(), RESOURCE.invalidSampleSize());
                 }

                 // Treat TABLESAMPLE(0) and TABLESAMPLE(100) as no table
                 // sampling at all.  Not strictly correct: TABLESAMPLE(0)
                 // should produce no output, but it simplifies implementation
                 // to know that some amount of sampling will occur.
                 // In practice values less than ~1E-43% are treated as 0.0 and
                 // values greater than ~99.999997% are treated as 1.0
                 float fRate = rate.divide(ONE_HUNDRED).floatValue();
                 if (fRate > 0.0f && fRate < 1.0f) {
                     SqlSampleSpec tableSampleSpec =
                     isRepeatable
                         ? SqlSampleSpec.createTableSample(
                             isBernoulli, fRate, repeatableSeed)
                         : SqlSampleSpec.createTableSample(isBernoulli, fRate);

                     SqlLiteral tableSampleLiteral =
                         SqlLiteral.createSample(tableSampleSpec, s2.end(this));
                     tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                         s2.end(this), tableRef, tableSampleLiteral);
                 }
             }
         )
     ]
     {
           return tableRef;
     }
 }
