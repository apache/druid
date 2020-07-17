/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Hand picked from https://druid.apache.org/docs/latest/querying/sql.html

exports.SQL_KEYWORDS = [
  'EXPLAIN PLAN FOR',
  'WITH',
  'AS',
  'SELECT',
  'ALL',
  'DISTINCT',
  'FROM',
  'WHERE',
  'GROUP BY',
  'HAVING',
  'ORDER BY',
  'ASC',
  'DESC',
  'LIMIT',
  'UNION ALL',
  'JOIN',
  'LEFT',
  'INNER',
  'ON',
  'RIGHT',
  'OUTER',
  'FULL',
];

exports.SQL_EXPRESSION_PARTS = [
  'FILTER',
  'END',
  'ELSE',
  'WHEN',
  'CASE',
  'OR',
  'AND',
  'NOT',
  'IN',
  'IS',
  'TO',
  'BETWEEN',
  'LIKE',
  'ESCAPE',
  'BOTH',
  'LEADING',
  'TRAILING',
  'EPOCH',
  'SECOND',
  'MINUTE',
  'HOUR',
  'DAY',
  'DOW',
  'DOY',
  'WEEK',
  'MONTH',
  'QUARTER',
  'YEAR',
  'TIMESTAMP',
  'INTERVAL',
];

exports.SQL_CONSTANTS = ['NULL', 'FALSE', 'TRUE'];

exports.SQL_DYNAMICS = [
  'CURRENT_TIMESTAMP',
  'CURRENT_DATE',
  'LOCALTIME',
  'LOCALTIMESTAMP',
  'CURRENT_TIME',
];
