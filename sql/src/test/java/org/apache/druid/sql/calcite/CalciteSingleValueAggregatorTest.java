package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class CalciteSingleValueAggregatorTest extends CalciteQueryTest {

    @Test
    public void testSingleValueFloatAgg()
    {
        msqIncompatible();
        skipVectorize();
        cannotVectorize();
        testQuery(
                "SELECT count(*) FROM foo where m1 >= (select max(m1) - 4 from foo)",
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(join(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new QueryDataSource(GroupByQuery.builder()
                                                .setDataSource(new QueryDataSource(
                                                        Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMaxAggregatorFactory("a0", "m1"))
                                                                .build()
                                                ))
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setVirtualColumns(expressionVirtualColumn(
                                                        "v0",
                                                        "(\"a0\" - 4)",
                                                        ColumnType.FLOAT
                                                        )
                                                )
                                                .setAggregatorSpecs(
                                                        aggregators(
                                                                new SingleValueFloatAggregatorFactory("_a0", "v0")
                                                        )
                                                )
                                                .setLimitSpec(NoopLimitSpec.instance())
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                        ),
                                        "j0.",
                                        "1",
                                        JoinType.INNER
                                ))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                .filters(expressionFilter("(\"m1\" >= \"j0._a0\")"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                ImmutableList.of(
                        new Object[]{5L}
                )
        );
    }

    @Test
    public void testSingleValueDoubleAgg()
    {
        msqIncompatible();
        skipVectorize();
        cannotVectorize();
        testQuery(
                "SELECT count(*) FROM foo where m1 >= (select max(m1) - 3.5 from foo)",
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(join(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new QueryDataSource(GroupByQuery.builder()
                                                .setDataSource(new QueryDataSource(
                                                        Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMaxAggregatorFactory("a0", "m1"))
                                                                .build()
                                                ))
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setVirtualColumns(expressionVirtualColumn(
                                                                "v0",
                                                                "(\"a0\" - 3.5)",
                                                                ColumnType.DOUBLE
                                                        )
                                                )
                                                .setAggregatorSpecs(
                                                        aggregators(
                                                                new SingleValueDoubleAggregatorFactory("_a0", "v0")
                                                        )
                                                )
                                                .setLimitSpec(NoopLimitSpec.instance())
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                        ),
                                        "j0.",
                                        "1",
                                        JoinType.INNER
                                ))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                .filters(expressionFilter("(\"m1\" >= \"j0._a0\")"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                ImmutableList.of(
                        new Object[]{4L}
                )
        );
    }

    @Test
    public void testSingleValueLongAgg()
    {
        msqIncompatible();
        skipVectorize();
        cannotVectorize();
        testQuery(
                "SELECT count(*) FROM wikipedia where __time >= (select max(__time) - INTERVAL '10' MINUTE from wikipedia)",
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(join(
                                        new TableDataSource(CalciteTests.WIKIPEDIA),
                                        new QueryDataSource(GroupByQuery.builder()
                                                .setDataSource(new QueryDataSource(
                                                        Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.WIKIPEDIA)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                                                .build()
                                                ))
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setVirtualColumns(expressionVirtualColumn(
                                                                "v0",
                                                        "(\"a0\" - 600000)",
                                                                ColumnType.LONG
                                                        )
                                                )
                                                .setAggregatorSpecs(
                                                        aggregators(
                                                                new SingleValueLongAggregatorFactory("_a0", "v0")
                                                        )
                                                )
                                                .setLimitSpec(NoopLimitSpec.instance())
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                        ),
                                        "j0.",
                                        "1",
                                        JoinType.INNER
                                ))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                .filters(expressionFilter("(\"__time\" >= \"j0._a0\")"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                ImmutableList.of(
                        new Object[]{220L}
                )
        );
    }
}
