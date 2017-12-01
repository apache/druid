/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.Pair;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class SQLMetadataViewManager implements MetadataViewManager {

    private final Object lock = new Object();
    private final SQLMetadataConnector connector;
    private final MetadataStorageTablesConfig config;
    private final IDBI dbi;

    private volatile boolean started = false;

    @Inject
    public SQLMetadataViewManager(SQLMetadataConnector connector, MetadataStorageTablesConfig config) {
        this.connector = connector;
        this.config = config;
        this.dbi = connector.getDBI();
    }

    // TODO: IMPORTANT - How am i going to call this init method. Using ManageLifecycle messes up the ordering of
    // initialization somehow.
    public void start()
    {
        synchronized (lock) {
            if (started) {
                return;
            }

            this.connector.createViewsTable();
            started = true;
        }
    }

    @Override
    public Void insertOrUpdate(String viewName, String viewSql) {
        return dbi.inTransaction(
            new TransactionCallback<Void>()
            {
                @Override
                public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
                {
                    int count = handle
                        .createQuery(
                            String.format("SELECT COUNT(*) FROM %1$s WHERE view_name = :view_name",
                                    config.getViewsTable())
                        )
                        .bind("view_name", viewName)
                        .map(IntegerMapper.FIRST)
                        .first();
                    if (count == 0) {
                        handle.createStatement(
                            String.format(
                                "INSERT INTO %1$s (view_name, view_sql) VALUES (:view_name, :view_sql)",
                                config.getViewsTable()
                            )
                        )
                        .bind("view_name", viewName)
                        .bind("view_sql", viewSql)
                        .execute();
                    } else {
                        handle.createStatement(
                            String.format(
                                    "UPDATE %1$s SET view_sql=:view_sql WHERE view_name=:view_name",
                                    config.getViewsTable()
                            )
                        )
                        .bind("view_name", viewName)
                        .bind("view_sql", viewSql)
                        .execute();
                    }

                    return null;
                }
            }
        );
    }

    @Override
    public Void remove(String viewName) {
        return dbi.inTransaction(
            new TransactionCallback<Void>() {
                @Override
                public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception {
                    handle.execute(
                            String.format("DELETE FROM %s WHERE view_name = '%s'", config.getViewsTable(), viewName));

                    return null;
                }
            }
        );
    }

    @Override
    public Map<String, String> getAll() {
        return dbi.withHandle(
            new HandleCallback<Map<String, String>>()
            {
                @Override
                public Map<String, String> withHandle(Handle handle) throws Exception
                {
                    return handle.createQuery(
                        String.format("SELECT view_name, view_sql FROM %s", config.getViewsTable())
                    )
                    .map(
                        new ResultSetMapper<Pair<String, String>>()
                        {
                            @Override
                            public Pair<String, String> map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException
                            {
                                return Pair.of(
                                        r.getString("view_name"),
                                        r.getString("view_sql")
                                );
                            }
                        }
                    )
                    .fold(
                        Maps.newHashMap(),
                        new Folder3<Map<String, String>, Pair<String, String>>()
                        {
                            @Override
                            public Map<String, String> fold(
                                    Map<String, String> druidViews,
                                    Pair<String, String> viewEntry,
                                    FoldController foldController,
                                    StatementContext statementContext
                            ) throws SQLException {
                                druidViews.put(viewEntry.lhs, viewEntry.rhs);
                                return druidViews;
                            }
                        }
                    );
                }
            }
        );
    }
}
