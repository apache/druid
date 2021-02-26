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

INSERT INTO druid_tasks (id, created_date, datasource, payload, status_payload, active) VALUES ('index_auth_test_2030-04-30T01:13:31.893Z', '2030-04-30T01:13:31.893Z', 'auth_test', '{\"id\":\"index_auth_test_2030-04-30T01:13:31.893Z\",\"created_date\":\"2030-04-30T01:13:31.893Z\",\"datasource\":\"auth_test\",\"active\":0}', '{\"id\":\"index_auth_test_2030-04-30T01:13:31.893Z\",\"status\":\"SUCCESS\",\"duration\":1}', 0);
INSERT INTO druid_segments (id,dataSource,created_date,start,end,partitioned,version,used,payload) VALUES ('auth_test_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9','auth_test','2013-03-15T20:49:52.348Z','2012-12-29T00:00:00.000Z','2013-01-10T08:00:00.000Z',0,'2013-01-10T08:13:47.830Z_v9',1,'{\"dataSource\":\"auth_test\",\"interval\":\"2012-12-29T00:00:00.000Z/2013-01-10T08:00:00.000Z\",\"version\":\"2013-01-10T08:13:47.830Z_v9\",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/wikipedia_editstream/2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z/2013-01-10T08:13:47.830Z_v9/0/index.zip\"},\"dimensions\":\"anonymous,area_code,city,continent_code,country_name,dma_code,geo,language,namespace,network,newpage,page,postal_code,region_lookup,robot,unpatrolled,user\",\"metrics\":\"added,count,deleted,delta,delta_hist,unique_users,variation\",\"shardSpec\":{\"type\":\"none\"},\"binaryVersion\":9,\"size\":446027801,\"identifier\":\"auth_test_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\"}');
