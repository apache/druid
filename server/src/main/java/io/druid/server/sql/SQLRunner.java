/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.CloseQuietly;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.sql.antlr4.DruidSQLLexer;
import io.druid.sql.antlr4.DruidSQLParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.TokenStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class SQLRunner
{
  private static final String STATEMENT = "select count(*), (1 - count(*) / sum(count)) * 100 as ratio from wikipedia where"
                                          + " timestamp between '2013-02-01' and '2013-02-14'"
                                          + " and (namespace = 'article' or page ~ 'Talk:.*')"
                                          + " and language in ( 'en', 'fr' ) "
                                          + " and user ~ '(?i)^david.*'"
                                          + " group by granularity(timestamp, 'day'), language";

  public static void main(String[] args) throws Exception
  {

    Options options = new Options();
    options.addOption("h", "help", false, "help");
    options.addOption("v", false, "verbose");
    options.addOption("e", "host", true, "endpoint [hostname:port]");

    CommandLine cmd = new GnuParser().parse(options, args);

    if(cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SQLRunner", options);
      System.exit(2);
    }

    String hostname = cmd.getOptionValue("e", "localhost:8080");
    String sql = cmd.getArgs().length > 0 ? cmd.getArgs()[0] : STATEMENT;

    ObjectMapper objectMapper = new DefaultObjectMapper();
    ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();

    CharStream stream = new ANTLRInputStream(sql);
    DruidSQLLexer lexer = new DruidSQLLexer(stream);
    TokenStream tokenStream = new CommonTokenStream(lexer);
    DruidSQLParser parser = new DruidSQLParser(tokenStream);
    lexer.removeErrorListeners();
    parser.removeErrorListeners();

    lexer.addErrorListener(ConsoleErrorListener.INSTANCE);
    parser.addErrorListener(ConsoleErrorListener.INSTANCE);

    try {
      DruidSQLParser.QueryContext queryContext = parser.query();
      if(parser.getNumberOfSyntaxErrors() > 0) throw new IllegalStateException();
//      parser.setBuildParseTree(true);
//      System.err.println(q.toStringTree(parser));
    } catch(Exception e) {
      String msg = e.getMessage();
      if(msg != null) System.err.println(e);
      System.exit(1);
    }

    final Query query;
    final TypeReference typeRef;
    boolean groupBy = false;
    if(parser.groupByDimensions.isEmpty()) {
      query = Druids.newTimeseriesQueryBuilder()
                  .dataSource(parser.getDataSource())
                  .aggregators(new ArrayList<AggregatorFactory>(parser.aggregators.values()))
                  .postAggregators(parser.postAggregators)
                  .intervals(parser.intervals)
                  .granularity(parser.granularity)
                  .filters(parser.filter)
                  .build();

      typeRef = new TypeReference<List<Result<TimeseriesResultValue>>>(){};
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

      typeRef = new TypeReference<List<Row>>(){};
      groupBy = true;
    }

    String queryStr = jsonWriter.writeValueAsString(query);
    if(cmd.hasOption("v")) System.err.println(queryStr);

    URL url = new URL(String.format("http://%s/druid/v2/?pretty", hostname));
    final URLConnection urlConnection = url.openConnection();
    urlConnection.addRequestProperty("content-type", "application/json");
    urlConnection.getOutputStream().write(queryStr.getBytes(Charsets.UTF_8));
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), Charsets.UTF_8));

    Object res = objectMapper.readValue(stdInput, typeRef);

    Joiner tabJoiner = Joiner.on("\t");

    if(groupBy) {
      List<Row> rows = (List<Row>)res;
      Iterable<String> dimensions = Iterables.transform(parser.groupByDimensions.values(), new Function<DimensionSpec, String>()
                        {
                          @Override
                          public String apply(@Nullable DimensionSpec input)
                          {
                            return input.getOutputName();
                          }
                        });

      System.out.println(tabJoiner.join(Iterables.concat(
                Lists.newArrayList("timestamp"),
                dimensions,
                parser.fields
            )));
      for(final Row r : rows) {
        System.out.println(
            tabJoiner.join(
              Iterables.concat(
                  Lists.newArrayList(parser.granularity.toDateTime(r.getTimestampFromEpoch())),
                  Iterables.transform(
                      parser.groupByDimensions.values(), new Function<DimensionSpec, String>()
                  {
                    @Override
                    public String apply(@Nullable DimensionSpec input)
                    {
                      return Joiner.on(",").join(r.getDimension(input.getOutputName()));
                    }
                  }),
                  Iterables.transform(parser.fields, new Function<String, Object>()
                  {
                    @Override
                    public Object apply(@Nullable String input)
                    {
                      return r.getFloatMetric(input);
                    }
                  })
              )
          )
        );
      }
    }
    else {
      List<Result<TimeseriesResultValue>> rows = (List<Result<TimeseriesResultValue>>)res;
      System.out.println(tabJoiner.join(Iterables.concat(
          Lists.newArrayList("timestamp"),
          parser.fields
      )));
      for(final Result<TimeseriesResultValue> r : rows) {
        System.out.println(
            tabJoiner.join(
              Iterables.concat(
                Lists.newArrayList(r.getTimestamp()),
                Lists.transform(
                  parser.fields,
                  new Function<String, Object>()
                  {
                    @Override
                    public Object apply(@Nullable String input)
                    {
                      return r.getValue().getMetric(input);
                    }
                  }
                )
              )
          )
        );
      }
    }

    CloseQuietly.close(stdInput);
  }
}
