package io.druid.server.lookup.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.Map;

public class PrefetchRangeQueryProvider implements PrefetchQueryProvider
{
  private String prefetchQueryFromTo;
  private String prefetchQueryFromOnly;
  private String prefetchQueryToOnly;

  PrefetchRangeQueryProvider()
  {

  }

  @Override
  public void init(String keyColumn, String valueColumn, String table)
  {
    this.prefetchQueryFromTo = String.format(
        "SELECT %s, %s FROM %s WHERE :from <= %s AND %s < :to",
        keyColumn,
        valueColumn,
        table,
        keyColumn,
        keyColumn
    );
    this.prefetchQueryFromOnly = String.format(
        "SELECT %s, %s FROM %s WHERE %s < :to",
        keyColumn,
        valueColumn,
        table,
        keyColumn
    );
    this.prefetchQueryToOnly = String.format(
        "SELECT %s, %s FROM %s WHERE :from <= %s",
        keyColumn,
        valueColumn,
        table,
        keyColumn
    );
  }

  @Override
  public Query<Map<String, Object>> makePrefetchQuery(Handle handle, String[] keys)
  {
    Preconditions.checkArgument(keys.length == 2,
        "Needs only two points of range (start and end) but got %d", keys.length);
    String from = keys[0];
    String to = keys[1];
    return
        (Strings.emptyToNull(from) == null) ? handle.createQuery(prefetchQueryToOnly).bind("to", to) :
            (Strings.emptyToNull(to) == null) ? handle.createQuery(prefetchQueryFromOnly).bind("from", from)
                : handle.createQuery(prefetchQueryFromTo).bind("from", from).bind("to", to);
  }
}
