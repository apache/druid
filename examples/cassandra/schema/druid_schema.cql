CREATE TABLE index_storage ( key text, chunk text, value blob, PRIMARY KEY (key, chunk)) WITH COMPACT STORAGE;
CREATE TABLE descriptor_storage ( key varchar, lastModified timestamp, descriptor varchar, PRIMARY KEY (key) ) WITH COMPACT STORAGE;
