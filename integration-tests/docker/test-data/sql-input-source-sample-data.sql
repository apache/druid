-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

create table sqlinputsource.wikipedia_index_data1(
    timestamp VARCHAR(100) NOT NULL,
    page VARCHAR(100),
    language VARCHAR(40),
    user VARCHAR(100),
    unpatrolled VARCHAR(100),
    newPage VARCHAR(40),
    robot VARCHAR(100),
    anonymous VARCHAR(100),
    namespace VARCHAR(40),
    continent VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(40),
    city VARCHAR(100),
    added INT,
    deleted INT,
    delta INT
);

create table sqlinputsource.wikipedia_index_data2(
    timestamp VARCHAR(100) NOT NULL,
    page VARCHAR(100),
    language VARCHAR(40),
    user VARCHAR(100),
    unpatrolled VARCHAR(100),
    newPage VARCHAR(40),
    robot VARCHAR(100),
    anonymous VARCHAR(100),
    namespace VARCHAR(40),
    continent VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(40),
    city VARCHAR(100),
    added INT,
    deleted INT,
    delta INT
);

create table sqlinputsource.wikipedia_index_data3(
    timestamp VARCHAR(100) NOT NULL,
    page VARCHAR(100),
    language VARCHAR(40),
    user VARCHAR(100),
    unpatrolled VARCHAR(100),
    newPage VARCHAR(40),
    robot VARCHAR(100),
    anonymous VARCHAR(100),
    namespace VARCHAR(40),
    continent VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(40),
    city VARCHAR(100),
    added INT,
    deleted INT,
    delta INT
);

create table sqlinputsource.wikipedia_index_data_all(
    timestamp VARCHAR(100) NOT NULL,
    page VARCHAR(100),
    language VARCHAR(40),
    user VARCHAR(100),
    unpatrolled VARCHAR(100),
    newPage VARCHAR(40),
    robot VARCHAR(100),
    anonymous VARCHAR(100),
    namespace VARCHAR(40),
    continent VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(40),
    city VARCHAR(100),
    added INT,
    deleted INT,
    delta INT
);

INSERT INTO sqlinputsource.wikipedia_index_data1 VALUES ("2013-08-31T01:02:33Z", "Gypsy Danger", "en", "nuclear", "true", "true", "false", "false", "article", "North America", "United States", "Bay Area", "San Francisco", 57, 200, -143);
INSERT INTO sqlinputsource.wikipedia_index_data1 VALUES ("2013-08-31T03:32:45Z", "Striker Eureka", "en", "speed", "false", "true", "true", "false", "wikipedia", "Australia", "Australia", "Cantebury", "Syndey", 459, 129, 330);
INSERT INTO sqlinputsource.wikipedia_index_data1 VALUES ("2013-08-31T07:11:21Z", "Cherno Alpha", "ru", "masterYi", "false", "true", "true", "false", "article", "Asia", "Russia", "Oblast", "Moscow", 123, 12, 111);

INSERT INTO sqlinputsource.wikipedia_index_data2 VALUES ("2013-08-31T11:58:39Z", "Crimson Typhoon", "zh", "triplets", "true", "false", "true", "false", "wikipedia", "Asia", "China", "Shanxi", "Taiyuan", 905, 5, 900);
INSERT INTO sqlinputsource.wikipedia_index_data2 VALUES ("2013-08-31T12:41:27Z", "Coyote Tango", "ja", "stringer", "true", "false", "true", "false", "wikipedia", "Asia", "Japan", "Kanto", "Tokyo", 1, 10, -9);
INSERT INTO sqlinputsource.wikipedia_index_data2 VALUES ("2013-09-01T01:02:33Z", "Gypsy Danger", "en", "nuclear", "true", "true", "false", "false", "article", "North America", "United States", "Bay Area", "San Francisco", 57, 200, -143);

INSERT INTO sqlinputsource.wikipedia_index_data3 VALUES ("2013-09-01T03:32:45Z", "Striker Eureka", "en", "speed", "false", "true", "true", "false", "wikipedia", "Australia", "Australia", "Cantebury", "Syndey", 459, 129, 330);
INSERT INTO sqlinputsource.wikipedia_index_data3 VALUES ("2013-09-01T07:11:21Z", "Cherno Alpha", "ru", "masterYi", "false", "true", "true", "false", "article", "Asia", "Russia", "Oblast", "Moscow", 123, 12, 111);
INSERT INTO sqlinputsource.wikipedia_index_data3 VALUES ("2013-09-01T11:58:39Z", "Crimson Typhoon", "zh", "triplets", "true", "false", "true", "false", "wikipedia", "Asia", "China", "Shanxi", "Taiyuan", 905, 5, 900);
INSERT INTO sqlinputsource.wikipedia_index_data3 VALUES ("2013-09-01T12:41:27Z", "Coyote Tango", "ja", "stringer", "true", "false", "true", "false", "wikipedia", "Asia", "Japan", "Kanto", "Tokyo", 1, 10, -9);

INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-08-31T01:02:33Z", "Gypsy Danger", "en", "nuclear", "true", "true", "false", "false", "article", "North America", "United States", "Bay Area", "San Francisco", 57, 200, -143);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-08-31T03:32:45Z", "Striker Eureka", "en", "speed", "false", "true", "true", "false", "wikipedia", "Australia", "Australia", "Cantebury", "Syndey", 459, 129, 330);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-08-31T07:11:21Z", "Cherno Alpha", "ru", "masterYi", "false", "true", "true", "false", "article", "Asia", "Russia", "Oblast", "Moscow", 123, 12, 111);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-08-31T11:58:39Z", "Crimson Typhoon", "zh", "triplets", "true", "false", "true", "false", "wikipedia", "Asia", "China", "Shanxi", "Taiyuan", 905, 5, 900);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-08-31T12:41:27Z", "Coyote Tango", "ja", "stringer", "true", "false", "true", "false", "wikipedia", "Asia", "Japan", "Kanto", "Tokyo", 1, 10, -9);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-09-01T01:02:33Z", "Gypsy Danger", "en", "nuclear", "true", "true", "false", "false", "article", "North America", "United States", "Bay Area", "San Francisco", 57, 200, -143);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-09-01T03:32:45Z", "Striker Eureka", "en", "speed", "false", "true", "true", "false", "wikipedia", "Australia", "Australia", "Cantebury", "Syndey", 459, 129, 330);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-09-01T07:11:21Z", "Cherno Alpha", "ru", "masterYi", "false", "true", "true", "false", "article", "Asia", "Russia", "Oblast", "Moscow", 123, 12, 111);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-09-01T11:58:39Z", "Crimson Typhoon", "zh", "triplets", "true", "false", "true", "false", "wikipedia", "Asia", "China", "Shanxi", "Taiyuan", 905, 5, 900);
INSERT INTO sqlinputsource.wikipedia_index_data_all VALUES ("2013-09-01T12:41:27Z", "Coyote Tango", "ja", "stringer", "true", "false", "true", "false", "wikipedia", "Asia", "Japan", "Kanto", "Tokyo", 1, 10, -9);

