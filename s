2024-12-04T16:38:13,926 INFO [main] org.apache.druid.sql.calcite.planner.CalciteRulesManager - __decAfterTrim1
LogicalProject(arrayString=[CAST(ARRAY('a':VARCHAR, 'b':VARCHAR)):VARCHAR ARRAY], uln=[$1], udn=[$2], usn=[$3]): rowcount = 3.75, cumulative cost = {inf}, id = 1092
  LogicalCorrelate(correlation=[$cor2], joinType=[inner], requiredColumns=[{0}]): rowcount = 3.75, cumulative cost = {inf}, id = 1091
    LogicalProject(arrayStringNulls=[$0], uln=[$2], udn=[$3]): rowcount = 3.75, cumulative cost = {inf}, id = 1085
      LogicalFilter(condition=[OR(=($2, 1), =($3, 2.2))]): rowcount = 3.75, cumulative cost = {inf}, id = 1084
        LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}]): rowcount = 15.0, cumulative cost = {inf}, id = 1083
          LogicalProject(arrayStringNulls=[$0], arrayDoubleNulls=[$2], uln=[$3]): rowcount = 15.0, cumulative cost = {inf}, id = 1079
            LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}]): rowcount = 15.0, cumulative cost = {inf}, id = 1078
              LogicalProject(arrayStringNulls=[$1], arrayLongNulls=[$2], arrayDoubleNulls=[$3]): rowcount = 15.0, cumulative cost = {230.0 rows, 646.0 cpu, 0.0 io}, id = 1074
                LogicalFilter(condition=[=($0, CAST(ARRAY('a', 'b')):VARCHAR ARRAY NOT NULL)]): rowcount = 15.0, cumulative cost = {215.0 rows, 601.0 cpu, 0.0 io}, id = 1073
                  LogicalProject(arrayString=[$1], arrayStringNulls=[$2], arrayLongNulls=[$4], arrayDoubleNulls=[$6]): rowcount = 100.0, cumulative cost = {200.0 rows, 501.0 cpu, 0.0 io}, id = 1072
                    LogicalTableScan(table=[[druid, arrays]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 640
              Uncollect: rowcount = 1.0, cumulative cost = {3.0 rows, 3.0 cpu, 0.0 io}, id = 1077
                LogicalProject(arrayLongNulls=[$cor0.arrayLongNulls]): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 0.0 io}, id = 1076
                  LogicalValues(tuples=[[{ 0 }]]): rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0 io}, id = 641
          Uncollect: rowcount = 1.0, cumulative cost = {3.0 rows, 3.0 cpu, 0.0 io}, id = 1082
            LogicalProject(arrayDoubleNulls=[$cor1.arrayDoubleNulls]): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 0.0 io}, id = 1081
              LogicalValues(tuples=[[{ 0 }]]): rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0 io}, id = 647
    LogicalFilter(condition=[=($0, 'a')]): rowcount = 1.0, cumulative cost = {4.0 rows, 4.0 cpu, 0.0 io}, id = 1090
      Uncollect: rowcount = 1.0, cumulative cost = {3.0 rows, 3.0 cpu, 0.0 io}, id = 1089
        LogicalProject(arrayStringNulls=[$cor2.arrayStringNulls]): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 0.0 io}, id = 1088
          LogicalValues(tuples=[[{ 0 }]]): rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0 io}, id = 653
