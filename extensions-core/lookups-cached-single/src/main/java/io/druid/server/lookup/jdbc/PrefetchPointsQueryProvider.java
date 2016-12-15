package io.druid.server.lookup.jdbc;

import com.google.common.base.Preconditions;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.Map;

public class PrefetchPointsQueryProvider implements PrefetchQueryProvider
{
  private String keyColumn;
  private String valueColumn;
  private String table;

  protected PrefetchPointsQueryProvider()
  {

  }

  @Override
  public void init(String keyColumn, String valueColumn, String table)
  {
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.table = table;
  }

  @Override
  public Query<Map<String, Object>> makePrefetchQuery(Handle handle, String[] keys)
  {
    Preconditions.checkArgument(keys.length > 0, "Needs at least one key to prefetch");
    return bindKeysToQuery(handle.createQuery(makeQueryForKeys(keys.length)), keys);
  }

  private String makeQueryForKeys(int numberOfKeys)
  {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("SELECT %s, %s FROM %s WHERE %s in (", keyColumn, valueColumn, table, keyColumn));
    for (int index = 0; index < numberOfKeys; index++)
    {
      if (index != 0) {
        builder.append(", ");
      }
      builder.append(":val").append(index + 1);
    }
    builder.append(")");

    return builder.toString();
  }

  private Query<Map<String, Object>> bindKeysToQuery(Query<Map<String, Object>> query, String[] keys)
  {
    int index = 1;
    for (String key: keys) {
      String bindArg = "val" + index;
      query.bind(bindArg, key);
      index++;
    }

    return query;
  }
}
