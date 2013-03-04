package com.metamx.druid.sql;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.metamx.druid.Druids;
import com.metamx.druid.Query;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.sql.antlr4.DruidSQLLexer;
import com.metamx.druid.sql.antlr4.DruidSQLParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class SQLRunner
{
  private static final String STATEMENT = "select count(*), sum(count) / count(*) as avg from wikipedia where"
                                          + " timestamp between '2013-02-01' and '2013-02-14'"
                                          + " and namespace = 'article'"
                                          + " and ( language = 'en' or language = 'fr' ) "
                                          + " group by granularity(timestamp, 'day'), language";

  public static void main(String[] args) throws Exception
  {
    String hostname = args.length > 0 ? args[0] : "localhost";
    String sql = args.length > 1 ? args[1] : STATEMENT;

    ObjectWriter json = new DefaultObjectMapper().writerWithDefaultPrettyPrinter();

    CharStream stream = new ANTLRInputStream(sql);
    DruidSQLLexer lexer = new DruidSQLLexer(stream);
    TokenStream tokenStream = new CommonTokenStream(lexer);
    DruidSQLParser parser = new DruidSQLParser(tokenStream);


    DruidSQLParser.QueryContext q = parser.query();

    parser.setBuildParseTree(true);
    System.err.println(q.toStringTree(parser));

    Query query;

    if(parser.groupByDimensions.isEmpty()) {
      query = Druids.newTimeseriesQueryBuilder()
                  .dataSource(parser.getDataSource())
                  .aggregators(new ArrayList<AggregatorFactory>(parser.aggregators.values()))
                  .postAggregators(parser.postAggregators)
                  .intervals(parser.intervals)
                  .granularity(parser.granularity)
                  .filters(parser.filter)
                  .build();
    } else {
      query = GroupByQuery.builder()
              .setDataSource(parser.getDataSource())
              .setAggregatorSpecs(new ArrayList<AggregatorFactory>(parser.aggregators.values()))
              .setPostAggregatorSpecs(parser.postAggregators)
              .setInterval(parser.intervals)
              .setGranularity(parser.granularity)
              .setDimFilter(parser.filter)
              .setDimensions(new ArrayList<DimensionSpec>(parser.groupByDimensions.values()))
              .build();
    }

    String queryStr = json.writeValueAsString(query);
    System.err.println(queryStr);

    PostMethod req = new PostMethod("http://" + hostname + "/druid/v2/?pretty");
    req.setRequestEntity(new StringRequestEntity(queryStr, "application/json", "utf-8"));
    new HttpClient().executeMethod(req);

    BufferedReader stdInput = new BufferedReader(new
                     InputStreamReader(req.getResponseBodyAsStream()));

    String s; while ((s = stdInput.readLine()) != null) System.out.println(s);

    stdInput.close();
  }
}
