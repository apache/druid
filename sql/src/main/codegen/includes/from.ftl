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

/*
 * Druid note: this file is copied from core/src/main/codegen/templates/Parser.jj in Calcite 1.35.0, with changes to
 * to add two elements of Druid syntax to the FROM clause:
 *
 * id [ (<args>) ]
 *
 * And
 *
 * TABLE(<fn>(<args>)) (<schema>)
 *
 * These changes were originally in https://github.com/apache/druid/pull/13360 as a patch script (sql/edit-parser.py),
 * then later moved to this copied-and-edited file in https://github.com/apache/druid/pull/13553.*
 *
 * This file prefixes the required production rules with 'Druid' so that the whole FROM production rule can be
 * derived from this file itself. The production clause is injected in the grammar using the maven replace plugin in
 * sql module's pom.
 */

/**
 * Parses the FROM clause for a SELECT.
 *
 * <p>FROM is mandatory in standard SQL, optional in dialects such as MySQL,
 * PostgreSQL. The parser allows SELECT without FROM, but the validator fails
 * if conformance is, say, STRICT_2003.
 */
SqlNode DruidFromClause() :
{
    SqlNode e, e2;
    SqlLiteral joinType;
}
{
    e = DruidJoin()
    (
        // Comma joins should only occur at top-level in the FROM clause.
        // Valid:
        //  * FROM a, b
        //  * FROM (a CROSS JOIN b), c
        // Not valid:
        //  * FROM a CROSS JOIN (b, c)
        LOOKAHEAD(1)
        <COMMA> { joinType = JoinType.COMMA.symbol(getPos()); }
        e2 = DruidJoin() {
            e = new SqlJoin(joinType.getParserPosition(),
                e,
                SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                joinType,
                e2,
                JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                null);
        }
    )*
    { return e; }
}

SqlNode DruidJoin() :
{
    SqlNode e;
}
{
    e = DruidTableRef1(ExprContext.ACCEPT_QUERY_OR_JOIN)
    (
        LOOKAHEAD(2)
        e = DruidJoinTable(e)
    )*
    {
        return e;
    }
}

/** Matches "LEFT JOIN t ON ...", "RIGHT JOIN t USING ...", "JOIN t". */
SqlNode DruidJoinTable(SqlNode e) :
{
    SqlNode e2, condition;
    final SqlLiteral natural, joinType, on, using;
    SqlNodeList list;
}
{
    // LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2) because JavaCC
    // calculates minimum lookahead count incorrectly for choice that contains
    // zero size child. For instance, with the generated code,
    // "LOOKAHEAD(2, Natural(), JoinType())"
    // returns true immediately if it sees a single "<CROSS>" token. Where we
    // expect the lookahead succeeds after "<CROSS> <APPLY>".
    //
    // For more information about the issue,
    // see https://github.com/javacc/javacc/issues/86
    //
    // We allow CROSS JOIN (joinType = CROSS_JOIN) to have a join condition,
    // even though that is not valid SQL; the validator will catch it.
    LOOKAHEAD(3)
    natural = Natural()
    joinType = JoinType()
    e2 = DruidTableRef1(ExprContext.ACCEPT_QUERY_OR_JOIN)
    (
        <ON> { on = JoinConditionType.ON.symbol(getPos()); }
        condition = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                on,
                condition);
        }
    |
        <USING> { using = JoinConditionType.USING.symbol(getPos()); }
        list = ParenthesizedSimpleIdentifierList() {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                using,
                new SqlNodeList(list, Span.of(using).end(this)));
        }
    |
        {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                JoinConditionType.NONE.symbol(joinType.getParserPosition()),
                null);
        }
    )
|
    <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
    e2 = DruidTableRef2(true) {
        if (!this.conformance.isApplyAllowed()) {
            throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
        }
        return new SqlJoin(joinType.getParserPosition(),
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
        return new SqlJoin(joinType.getParserPosition(),
            e,
            SqlLiteral.createBoolean(false, joinType.getParserPosition()),
            joinType,
            e2,
            JoinConditionType.ON.symbol(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(true, joinType.getParserPosition()));
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
    e = DruidTableRef3(ExprContext.ACCEPT_QUERY, false) { return e; }
}

SqlNode DruidTableRef1(ExprContext exprContext) :
{
    final SqlNode e;
}
{
    e = DruidTableRef3(exprContext, false) { return e; }
}

/**
 * Parses a table reference in a FROM clause.
 */
SqlNode DruidTableRef2(boolean lateral) :
{
    final SqlNode e;
}
{
    e = DruidTableRef3(ExprContext.ACCEPT_QUERY, lateral) { return e; }
}

SqlNode DruidTableRef3(ExprContext exprContext, boolean lateral) :
{
    final SqlIdentifier tableName;
    SqlNode tableRef;
    List<SqlNode> paramList;
    final SqlIdentifier alias;
    final Span s;
    SqlNodeList args;
    final SqlNodeList columnAliasList;
    SqlUnnestOperator unnestOp = SqlStdOperatorTable.UNNEST;
    SqlNodeList extendList = null;
}
{
    (
        LOOKAHEAD(2)
        tableName = CompoundTableIdentifier()
        ( tableRef = TableHints(tableName) | { tableRef = tableName; } )
        // BEGIN: Druid-specific code
        [
            paramList = FunctionParameterList(ExprContext.ACCEPT_NONCURSOR)
            {
                tableRef = ParameterizeOperator.PARAM.createCall(tableRef, paramList);
            }
        ]
        // END: Druid-specific code
        tableRef = Over(tableRef)
        [ tableRef = Snapshot(tableRef) ]
        [ tableRef = MatchRecognize(tableRef) ]
    |
        LOOKAHEAD(2)
        [ <LATERAL> { lateral = true; } ]
        tableRef = ParenthesizedExpression(exprContext)
        tableRef = Over(tableRef)
        tableRef = addLateral(tableRef, lateral)
        [ tableRef = MatchRecognize(tableRef) ]
    |
        <UNNEST> { s = span(); }
        args = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
        [
            <WITH> <ORDINALITY> {
                unnestOp = SqlStdOperatorTable.UNNEST_WITH_ORDINALITY;
            }
        ]
        {
            tableRef = unnestOp.createCall(s.end(this), (List<SqlNode>) args);
        }
    |
        [ <LATERAL> { lateral = true; } ]
        tableRef = TableFunctionCall()
        // BEGIN: Druid-specific code
        [
            [ <EXTEND> ]
            extendList = ExtendList()
            {
                tableRef = ExtendOperator.EXTEND.createCall(
                      Span.of(tableRef, extendList).pos(), tableRef, extendList);
            }
        ]
        // END: Druid-specific code
        tableRef = addLateral(tableRef, lateral)
    |
        tableRef = ExtendedTableRef()
    )
    [
        LOOKAHEAD(2)
        tableRef = Pivot(tableRef)
    ]
    [
        LOOKAHEAD(2)
        tableRef = Unpivot(tableRef)
    ]
    [
        [ <AS> ] alias = SimpleIdentifier()
        (
            columnAliasList = ParenthesizedSimpleIdentifierList()
        |   { columnAliasList = null; }
        )
        {
            // Standard SQL (and Postgres) allow applying "AS alias" to a JOIN,
            // e.g. "FROM (a CROSS JOIN b) AS c". The new alias obscures the
            // internal aliases, and columns cannot be referenced if they are
            // not unique. TODO: Support this behavior; see
            // [CALCITE-5168] Allow AS after parenthesized JOIN
            checkNotJoin(tableRef);
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
    [ tableRef = Tablesample(tableRef) ]
    { return tableRef; }
}
