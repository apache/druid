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

import { InputSource } from '../../../druid-models';

export interface ExampleInputSource {
  name: string;
  description: string;
  inputSource: InputSource;
}

export const EXAMPLE_INPUT_SOURCES: ExampleInputSource[] = [
  {
    name: 'Wikipedia',
    description: 'One day of wikipedia edits (JSON)',
    inputSource: {
      type: 'http',
      uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
    },
  },
  {
    name: 'KoalasToTheMax one day',
    description: 'One day of flat events from KoalasToTheMax.com (JSON)',
    inputSource: {
      type: 'http',
      uris: ['https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz'],
    },
  },
  {
    name: 'KoalasToTheMax one day (nested)',
    description: 'One day of nested events from KoalasToTheMax.com (JSON)',
    inputSource: {
      type: 'http',
      uris: [
        'https://static.imply.io/example-data/kttm-nested-v2/kttm-nested-v2-2019-08-25.json.gz',
      ],
    },
  },
  {
    name: 'NYC Taxi cabs (3 files)',
    description: '60M rows of NYC taxi trip data (CSV)',
    inputSource: {
      type: 'http',
      uris: [
        'https://static.imply.io/example-data/trips/trips_xaa.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xab.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xac.csv.gz',
      ],
    },
  },
  {
    name: 'NYC Taxi cabs (all files)',
    description: '1.4B rows of NYC taxi trip data (CSV)',
    inputSource: {
      type: 'http',
      uris: [
        'https://static.imply.io/example-data/trips/trips_xaa.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xab.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xac.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xad.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xae.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xaf.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xag.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xah.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xai.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xaj.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xak.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xal.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xam.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xan.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xao.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xap.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xaq.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xar.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xas.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xat.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xau.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xav.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xaw.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xax.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xay.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xaz.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xba.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbb.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbc.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbd.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbe.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbf.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbg.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbh.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbi.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbj.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbk.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbl.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbm.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbn.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbo.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbp.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbq.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbr.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbs.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbt.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbu.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbv.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbw.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbx.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xby.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xbz.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xca.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcb.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcc.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcd.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xce.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcf.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcg.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xch.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xci.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcj.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xck.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcl.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcm.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcn.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xco.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcp.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcq.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcr.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcs.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xct.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcu.csv.gz',
        'https://static.imply.io/example-data/trips/trips_xcv.csv.gz',
      ],
    },
  },
  {
    name: 'FlightCarrierOnTime (1 month)',
    description: 'One month of flight "On Time Performance" data (CSV)',
    inputSource: {
      type: 'http',
      uris: [
        'https://static.imply.io/example-data/flight_on_time/flights/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2005_11.csv.zip',
      ],
    },
  },
];
