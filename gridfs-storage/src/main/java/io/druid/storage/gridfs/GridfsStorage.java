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

package io.druid.storage.gridfs;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;

import java.net.UnknownHostException;

/**
 * Superclass for accessing Cassandra Storage.
 * <p/>
 * This is the schema used to support the index and descriptor storage:
 * <p/>
 * CREATE TABLE index_storage ( key text, chunk text, value blob, PRIMARY KEY (key, chunk)) WITH COMPACT STORAGE;
 * CREATE TABLE descriptor_storage ( key varchar, lastModified timestamp, descriptor varchar, PRIMARY KEY (key) ) WITH COMPACT STORAGE;
 */
public class GridfsStorage {
    private static final String COLLECTION_NAME = "druid_bucket";
    protected final GridFS gridFs;
    protected final GridfsDataSegmentConfig config;

    public GridfsStorage(GridfsDataSegmentConfig config) throws UnknownHostException {
        this.config = config;
        MongoClient mongo = new MongoClient(new MongoClientURI(config.uri));
        DB db = mongo.getDB(config.db);
        this.gridFs = new GridFS(db, config.bucket);
    }
}
