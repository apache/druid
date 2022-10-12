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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchApproxQuantileSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchObjectSqlAggregator;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class SqlBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final Logger log = new Logger(SqlBenchmark.class);

  private static final String STORAGE_MMAP = "mmap";
  private static final String STORAGE_FRAME_ROW = "frame-row";
  private static final String STORAGE_FRAME_COLUMNAR = "frame-columnar";

  private static final List<String> QUERIES = ImmutableList.of(
      // 0, 1, 2, 3: Timeseries, unfiltered
      "SELECT COUNT(*) FROM foo",
      "SELECT COUNT(DISTINCT hyper) FROM foo",
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo",
      "SELECT FLOOR(__time TO MINUTE), SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo GROUP BY 1",

      // 4: Timeseries, low selectivity filter (90% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential NOT LIKE '%3'",

      // 5: Timeseries, high selectivity filter (0.1% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential = '311'",

      // 6: Timeseries, mixing low selectivity index-capable filter (90% of rows match) + cursor filter
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3' AND maxLongUniform > 10",

      // 7: Timeseries, low selectivity toplevel filter (90%), high selectivity filtered aggregator (0.1%)
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal)\n"
      + "FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3'",

      // 8: Timeseries, no toplevel filter, various filtered aggregators with clauses repeated.
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumLongSequential),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumFloatNormal),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential = '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential <> '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  COUNT(*)\n"
      + "FROM foo",

      // 9: Timeseries, toplevel time filter, time-comparison filtered aggregators
      "SELECT\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-01 12:00:00'),\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 12:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00')\n"
      + "FROM foo\n"
      + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'",

      // 10, 11: GroupBy two strings, unfiltered, unordered
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1, 2",

      // 12, 13, 14: GroupBy one string, unfiltered, various aggregator configurations
      "SELECT dimZipf FROM foo GROUP BY 1",
      "SELECT dimZipf, COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",
      "SELECT dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",

      // 15, 16: GroupBy long, unfiltered, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1",

      // 17, 18: GroupBy long, filter by long, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      // 19: ultra mega union matrix
      "WITH matrix (dimZipf, dimSequential) AS (\n"
      + "  (\n"
      + "    SELECT '100', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '100'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '110', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '110'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '120', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '120'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '130', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '130'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '140', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '140'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '150', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '150'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '160', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '160'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '170', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '170'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '180', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '180'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '190', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '190'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '200', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '200'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '210', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '210'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '220', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '220'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '230', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '230'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '240', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '240'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '250', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '250'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '260', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '260'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '270', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '270'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '280', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '280'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '290', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '290'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '300', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '300'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '310', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '310'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '320', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '320'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '330', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '330'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '340', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '340'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '350', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '350'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '360', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '360'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '370', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '370'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '380', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '380'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT 'other', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE\n"
      + "      dimZipf NOT IN (\n"
      + "        '100', '110', '120', '130', '140', '150', '160', '170', '180', '190',\n"
      + "        '200', '210', '220', '230', '240', '250', '260', '270', '280', '290',\n"
      + "        '300', '310', '320', '330', '340', '350', '360', '370', '380'\n"
      + "      )\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + ")\n"
      + "SELECT * FROM matrix",

      // 20: GroupBy, doubles sketches
      "SELECT dimZipf, APPROX_QUANTILE_DS(sumFloatNormal, 0.5), DS_QUANTILES_SKETCH(maxLongUniform) "
      + "FROM foo "
      + "GROUP BY 1",

      //21: Order by with alias with large in filter
      "SELECT __time as t, dimSequential from foo "
      + " where (dimSequential in (select DISTINCT dimSequential from foo)) "
      + " order by 1 limit 1",

      //22: Order by without alias with large in filter
      "SELECT __time, dimSequential from foo "
      + " where (dimSequential in (select DISTINCT dimSequential from foo)) "
      + " order by 1 limit 1",

      //23: Group by and Order by with alias with large in filter nested query
      "SELECT __time as t, dimSequential from foo "
      + " where dimSequential in (select dimSequential from foo where "
      + " dimSequential in (select dimSequential from foo)) "
      + " group by 1,2 order by 1,2 limit 1",

      //24: Order by query with large in filters
      "SELECT __time as t, dimZipf "
      + " from foo "
      + " where dimSequential IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256', '257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276', '277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '302', '303', '304', '305', '306', '307', '308', '309', '310', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321', '322', '323', '324', '325', '326', '327', '328', '329', '330', '331', '332', '333', '334', '335', '336', '337', '338', '339', '340', '341', '342', '343', '344', '345', '346', '347', '348', '349', '350', '351', '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363', '364', '365', '366', '367', '368', '369', '370', '371', '372', '373', '374', '375', '376', '377', '378', '379', '380', '381', '382', '383', '384', '385', '386', '387', '388', '389', '390', '391', '392', '393', '394', '395', '396', '397', '398', '399', '400', '401', '402', '403', '404', '405', '406', '407', '408', '409', '410', '411', '412', '413', '414', '415', '416', '417', '418', '419', '420', '421', '422', '423', '424', '425', '426', '427', '428', '429', '430', '431', '432', '433', '434', '435', '436', '437', '438', '439', '440', '441', '442', '443', '444', '445', '446', '447', '448', '449', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459', '460', '461', '462', '463', '464', '465', '466', '467', '468', '469', '470', '471', '472', '473', '474', '475', '476', '477', '478', '479', '480', '481', '482', '483', '484', '485', '486', '487', '488', '489', '490', '491', '492', '493', '494', '495', '496', '497', '498', '499', '500', '501', '502', '503', '504', '505', '506', '507', '508', '509', '510', '511', '512', '513', '514', '515', '516', '517', '518', '519', '520', '521', '522', '523', '524', '525', '526', '527', '528', '529', '530', '531', '532', '533', '534', '535', '536', '537', '538', '539', '540', '541', '542', '543', '544', '545', '546', '547', '548', '549', '550', '551', '552', '553', '554', '555', '556', '557', '558', '559', '560', '561', '562', '563', '564', '565', '566', '567', '568', '569', '570', '571', '572', '573', '574', '575', '576', '577', '578', '579', '580', '581', '582', '583', '584', '585', '586', '587', '588', '589', '590', '591', '592', '593', '594', '595', '596', '597', '598', '599', '600', '601', '602', '603', '604', '605', '606', '607', '608', '609', '610', '611', '612', '613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624', '625', '626', '627', '628', '629', '630', '631', '632', '633', '634', '635', '636', '637', '638', '639', '640', '641', '642', '643', '644', '645', '646', '647', '648', '649', '650', '651', '652', '653', '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667', '668', '669', '670', '671', '672', '673', '674', '675', '676', '677', '678', '679', '680', '681', '682', '683', '684', '685', '686', '687', '688', '689', '690', '691', '692', '693', '694', '695', '696', '697', '698', '699', '700', '701', '702', '703', '704', '705', '706', '707', '708', '709', '710', '711', '712', '713', '714', '715', '716', '717', '718', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730', '731', '732', '733', '734', '735', '736', '737', '738', '739', '740', '741', '742', '743', '744', '745', '746', '747', '748', '749', '750', '751', '752', '753', '754', '755', '756', '757', '758', '759', '760', '761', '762', '763', '764', '765', '766', '767', '768', '769', '770', '771', '772', '773', '774', '775', '776', '777', '778', '779', '780', '781', '782', '783', '784', '785', '786', '787', '788', '789', '790', '791', '792', '793', '794', '795', '796', '797', '798', '799', '800', '801', '802', '803', '804', '805', '806', '807', '808', '809', '810', '811', '812', '813', '814', '815', '816', '817', '818', '819', '820', '821', '822', '823', '824', '825', '826', '827', '828', '829', '830', '831', '832', '833', '834', '835', '836', '837', '838', '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849', '850', '851', '852', '853', '854', '855', '856', '857', '858', '859', '860', '861', '862', '863', '864', '865', '866', '867', '868', '869', '870', '871', '872', '873', '874', '875', '876', '877', '878', '879', '880', '881', '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895', '896', '897', '898', '899', '900', '901', '902', '903', '904', '905', '906', '907', '908', '909', '910', '911', '912', '913', '914', '915', '916', '917', '918', '919', '920', '921', '922', '923', '924', '925', '926', '927', '928', '929', '930', '931', '932', '933', '934', '935', '936', '937', '938', '939', '940', '941', '942', '943', '944', '945', '946', '947', '948', '949', '950', '951', '952', '953', '954', '955', '956', '957', '958', '959', '960', '961', '962', '963', '964', '965', '966', '967', '968', '969', '970', '971', '972', '973', '974', '975', '976', '977', '978', '979', '980', '981', '982', '983', '984', '985', '986', '987', '988', '989', '990', '991', '992', '993', '994', '995', '996', '997', '998') "
      + " order by 1",

      //25. Nested subquery with large in filter
      "SELECT __time as t, hyper from foo "
      + " where dimUniform in ( "
      + " select dimUniform from foo "
      + " where dimSequential IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256', '257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276', '277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '302', '303', '304', '305', '306', '307', '308', '309', '310', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321', '322', '323', '324', '325', '326', '327', '328', '329', '330', '331', '332', '333', '334', '335', '336', '337', '338', '339', '340', '341', '342', '343', '344', '345', '346', '347', '348', '349', '350', '351', '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363', '364', '365', '366', '367', '368', '369', '370', '371', '372', '373', '374', '375', '376', '377', '378', '379', '380', '381', '382', '383', '384', '385', '386', '387', '388', '389', '390', '391', '392', '393', '394', '395', '396', '397', '398', '399', '400', '401', '402', '403', '404', '405', '406', '407', '408', '409', '410', '411', '412', '413', '414', '415', '416', '417', '418', '419', '420', '421', '422', '423', '424', '425', '426', '427', '428', '429', '430', '431', '432', '433', '434', '435', '436', '437', '438', '439', '440', '441', '442', '443', '444', '445', '446', '447', '448', '449', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459', '460', '461', '462', '463', '464', '465', '466', '467', '468', '469', '470', '471', '472', '473', '474', '475', '476', '477', '478', '479', '480', '481', '482', '483', '484', '485', '486', '487', '488', '489', '490', '491', '492', '493', '494', '495', '496', '497', '498', '499', '500', '501', '502', '503', '504', '505', '506', '507', '508', '509', '510', '511', '512', '513', '514', '515', '516', '517', '518', '519', '520', '521', '522', '523', '524', '525', '526', '527', '528', '529', '530', '531', '532', '533', '534', '535', '536', '537', '538', '539', '540', '541', '542', '543', '544', '545', '546', '547', '548', '549', '550', '551', '552', '553', '554', '555', '556', '557', '558', '559', '560', '561', '562', '563', '564', '565', '566', '567', '568', '569', '570', '571', '572', '573', '574', '575', '576', '577', '578', '579', '580', '581', '582', '583', '584', '585', '586', '587', '588', '589', '590', '591', '592', '593', '594', '595', '596', '597', '598', '599', '600', '601', '602', '603', '604', '605', '606', '607', '608', '609', '610', '611', '612', '613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624', '625', '626', '627', '628', '629', '630', '631', '632', '633', '634', '635', '636', '637', '638', '639', '640', '641', '642', '643', '644', '645', '646', '647', '648', '649', '650', '651', '652', '653', '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667', '668', '669', '670', '671', '672', '673', '674', '675', '676', '677', '678', '679', '680', '681', '682', '683', '684', '685', '686', '687', '688', '689', '690', '691', '692', '693', '694', '695', '696', '697', '698', '699', '700', '701', '702', '703', '704', '705', '706', '707', '708', '709', '710', '711', '712', '713', '714', '715', '716', '717', '718', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730', '731', '732', '733', '734', '735', '736', '737', '738', '739', '740', '741', '742', '743', '744', '745', '746', '747', '748', '749', '750', '751', '752', '753', '754', '755', '756', '757', '758', '759', '760', '761', '762', '763', '764', '765', '766', '767', '768', '769', '770', '771', '772', '773', '774', '775', '776', '777', '778', '779', '780', '781', '782', '783', '784', '785', '786', '787', '788', '789', '790', '791', '792', '793', '794', '795', '796', '797', '798', '799', '800', '801', '802', '803', '804', '805', '806', '807', '808', '809', '810', '811', '812', '813', '814', '815', '816', '817', '818', '819', '820', '821', '822', '823', '824', '825', '826', '827', '828', '829', '830', '831', '832', '833', '834', '835', '836', '837', '838', '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849', '850', '851', '852', '853', '854', '855', '856', '857', '858', '859', '860', '861', '862', '863', '864', '865', '866', '867', '868', '869', '870', '871', '872', '873', '874', '875', '876', '877', '878', '879', '880', '881', '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895', '896', '897', '898', '899', '900', '901', '902', '903', '904', '905', '906', '907', '908', '909', '910', '911', '912', '913', '914', '915', '916', '917', '918', '919', '920', '921', '922', '923', '924', '925', '926', '927', '928', '929', '930', '931', '932', '933', '934', '935', '936', '937', '938', '939', '940', '941', '942', '943', '944', '945', '946', '947', '948', '949', '950', '951', '952', '953', '954', '955', '956', '957', '958', '959', '960', '961', '962', '963', '964', '965', '966', '967', '968', '969', '970', '971', '972', '973', '974', '975', '976', '977', '978', '979', '980', '981', '982', '983', '984', '985', '986', '987', '988', '989', '990', '991', '992', '993', '994', '995', '996', '997', '998') "
      + " ) order by 1"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  @Param({"force"})
  private String vectorize;

  @Param({"0", "10", "18"})
  private String query;

  @Param({STORAGE_MMAP, STORAGE_FRAME_ROW, STORAGE_FRAME_COLUMNAR})
  private String storageType;

  private SqlEngine engine;
  @Nullable
  private PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final PlannerConfig plannerConfig = new PlannerConfig();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);
    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);

    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate);
    addSegmentToWalker(walker, dataSegment, index);
    closer.register(walker);

    final DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    plannerFactory = new PlannerFactory(
        rootSchema,
        createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );
  }

  private void addSegmentToWalker(
      final SpecificSegmentsQuerySegmentWalker walker,
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    if (STORAGE_MMAP.equals(storageType)) {
      walker.add(descriptor, new QueryableIndexSegment(index, descriptor.getId()));
    } else if (STORAGE_FRAME_ROW.equals(storageType)) {
      walker.add(
          descriptor,
          FrameTestUtil.adapterToFrameSegment(
              new QueryableIndexStorageAdapter(index),
              FrameType.ROW_BASED,
              descriptor.getId()
          )
      );
    } else if (STORAGE_FRAME_COLUMNAR.equals(storageType)) {
      walker.add(
          descriptor,
          FrameTestUtil.adapterToFrameSegment(
              new QueryableIndexStorageAdapter(index),
              FrameType.COLUMNAR,
              descriptor.getId()
          )
      );
    }
  }

  private static DruidOperatorTable createOperatorTable()
  {
    try {
      final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();
      extractionOperators.add(CalciteTests.INJECTOR.getInstance(QueryLookupOperatorConversion.class));
      final Set<SqlAggregator> aggregators = new HashSet<>();
      aggregators.add(CalciteTests.INJECTOR.getInstance(DoublesSketchApproxQuantileSqlAggregator.class));
      aggregators.add(CalciteTests.INJECTOR.getInstance(DoublesSketchObjectSqlAggregator.class));
      final ApproxCountDistinctSqlAggregator countDistinctSqlAggregator =
          new ApproxCountDistinctSqlAggregator(new HllSketchApproxCountDistinctSqlAggregator());
      aggregators.add(new CountSqlAggregator(countDistinctSqlAggregator));
      aggregators.add(countDistinctSqlAggregator);
      return new DruidOperatorTable(aggregators, extractionOperators);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void querySql(Blackhole blackhole) throws Exception
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, new QueryContext(context))) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void planSql(Blackhole blackhole) throws Exception
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, new QueryContext(context))) {
      final PlannerResult plannerResult = planner.plan();
      blackhole.consume(plannerResult);
    }
  }
}
