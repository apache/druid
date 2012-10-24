package com.metamx.druid.db;

import com.metamx.common.logger.Logger;
import org.apache.commons.dbcp.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 */
public class DbConnector
{
  private static final Logger log = new Logger(DbConnector.class);

  public static void createSegmentTable(final DBI dbi, final DbConnectorConfig config)
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              List<Map<String, Object>> table = handle.select(
                  String.format(
                      "SHOW tables LIKE '%s'",
                      config.getSegmentTable()
                  )
              );

              if (table.isEmpty()) {
                log.info("Creating table[%s]", config.getSegmentTable());
                handle.createStatement(
                    String.format(
                        "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id))",
                        config.getSegmentTable()
                    )
                ).execute();
              } else {
                log.info("Table[%s] existed: [%s]", config.getSegmentTable(), table);
              }

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.warn(e.toString());
    }
  }

  private final DbConnectorConfig config;
  private final DBI dbi;

  public DbConnector(DbConnectorConfig config)
  {
    this.config = config;

    this.dbi = new DBI(getDatasource());
  }

  public DBI getDBI()
  {
    return dbi;
  }

  private DataSource getDatasource()
  {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(config.getDatabaseUser());
    dataSource.setPassword(config.getDatabasePassword());
    dataSource.setUrl(config.getDatabaseConnectURI());

    return dataSource;
  }
}
