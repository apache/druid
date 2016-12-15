package io.druid.server.lookup.jdbc;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "range", value = PrefetchRangeQueryProvider.class),
    @JsonSubTypes.Type(name = "points", value = PrefetchPointsQueryProvider.class)
})
public interface PrefetchQueryProvider
{
  void init(String keyColumn, String valueColumn, String table);
  Query<Map<String, Object>> makePrefetchQuery(Handle handle, String[] keys);
}
