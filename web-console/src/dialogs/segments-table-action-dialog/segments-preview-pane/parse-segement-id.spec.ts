/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { parseSegmentId } from './segments-preview-pane';

describe('parseSegmentId', () => {
  it('correctly identifies segment ID parts', () => {
    const segmentId =
      'kttm_reingest_2019-08-25T23:00:00.000Z_2019-08-26T00:00:00.000Z_2022-08-02T18:58:41.697Z';
    expect(parseSegmentId(segmentId).datasource).toEqual('kttm_reingest');
    expect(parseSegmentId(segmentId).interval).toEqual(
      '2019-08-25T23:00:00.000Z/2019-08-26T00:00:00.000Z',
    );
    expect(parseSegmentId(segmentId).version).toEqual('2022-08-02T18:58:41.697Z');
    expect(parseSegmentId(segmentId).partitionNumber).toEqual(0);
  });

  it('correctly identifies segment ID parts with partitionNumber', () => {
    const segmentId =
      'test_segment_id1_2019-08-25T23:00:00.000Z_2019-08-26T00:00:00.000Z_2022-08-02T18:58:41.697Z_1';
    expect(parseSegmentId(segmentId).datasource).toEqual('test_segment_id1');
    expect(parseSegmentId(segmentId).interval).toEqual(
      '2019-08-25T23:00:00.000Z/2019-08-26T00:00:00.000Z',
    );
    expect(parseSegmentId(segmentId).version).toEqual('2022-08-02T18:58:41.697Z');
    expect(parseSegmentId(segmentId).partitionNumber).toEqual(1);
  });

  it('correctly identifies segment ID parts with without partition number and _ in name', () => {
    const segmentId =
      'test___2019-08-25T23:00:00.000Z_2019-08-26T00:00:00.000Z_2022-08-02T18:58:41.697Z';
    expect(parseSegmentId(segmentId).datasource).toEqual('test__');
    expect(parseSegmentId(segmentId).interval).toEqual(
      '2019-08-25T23:00:00.000Z/2019-08-26T00:00:00.000Z',
    );
    expect(parseSegmentId(segmentId).version).toEqual('2022-08-02T18:58:41.697Z');
    expect(parseSegmentId(segmentId).partitionNumber).toEqual(0);
  });

  it('correctly identifies segment ID parts with long partition number', () => {
    const segmentId =
      'test___2019-08-25T23:00:00.000Z_2019-08-26T00:00:00.000Z_2022-08-02T18:58:41.697Z_1234567';
    expect(parseSegmentId(segmentId).datasource).toEqual('test__');
    expect(parseSegmentId(segmentId).interval).toEqual(
      '2019-08-25T23:00:00.000Z/2019-08-26T00:00:00.000Z',
    );
    expect(parseSegmentId(segmentId).version).toEqual('2022-08-02T18:58:41.697Z');
    expect(parseSegmentId(segmentId).partitionNumber).toEqual(1234567);
  });
});
