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

import { Icon, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { fromDate, getLocalTimeZone } from '@internationalized/date';
import { useMemo } from 'react';

import { minute, tickIcon, timezoneOffsetInMinutesToString } from '../../../utils';

const NAMED_TIMEZONES: string[] = [
  'America/Juneau', // -9:00
  'America/Los_Angeles', // -8:00
  'America/Yellowknife', // -7:00
  'America/Phoenix', // -7:00
  'America/Denver', // -7:00
  'America/Mexico_City', // -6:00
  'America/Chicago', // -6:00
  'America/New_York', // -5:00
  'America/Argentina/Buenos_Aires', // -4:00
  'Etc/UTC', // +0:00
  'Europe/London', // +0:00
  'Europe/Paris', // +1:00
  'Asia/Jerusalem', // +2:00
  'Asia/Kolkata', // +5:30
  'Asia/Shanghai', // +8:00
  'Asia/Hong_Kong', // +8:00
  'Asia/Seoul', // +9:00
  'Asia/Tokyo', // +9:00
  'Pacific/Guam', // +10:00
  'Australia/Sydney', // +11:00
];

export interface TimezoneMenuItemsProps {
  sqlTimeZone: string | undefined;
  setSqlTimeZone(sqlTimeZone: string | undefined): void;
  defaultSqlTimeZone: string | undefined;
}

export const TimezoneMenuItems = function TimezoneMenuItems(props: TimezoneMenuItemsProps) {
  const { sqlTimeZone, setSqlTimeZone, defaultSqlTimeZone } = props;

  const { timezones, myTimezone, offsetsInMinutes, myOffsetInMinutes } = useMemo(() => {
    const now = new Date();

    const timezones: { timezone: string; offsetInMinutes: number }[] = NAMED_TIMEZONES.map(
      timezone => ({
        timezone,
        offsetInMinutes: fromDate(now, timezone).offset / minute.canonicalLength,
      }),
    );
    const myTimezone = getLocalTimeZone();
    const myOffsetInMinutes = -now.getTimezoneOffset();

    // Make sure the browsers timezone is in the list
    if (!timezones.find(({ timezone }) => timezone === myTimezone)) {
      timezones.push({
        timezone: myTimezone,
        offsetInMinutes: myOffsetInMinutes,
      });
    }
    timezones.sort((a, b) => a.offsetInMinutes - b.offsetInMinutes);

    const offsetsInMinutes: number[] = [];
    for (let offsetInMinutes = -12 * 60; offsetInMinutes <= 14 * 60; offsetInMinutes += 60) {
      offsetsInMinutes.push(offsetInMinutes);
    }

    // Make sure the browser offset is in the list
    if (!offsetsInMinutes.includes(myOffsetInMinutes)) {
      offsetsInMinutes.push(myOffsetInMinutes);
      offsetsInMinutes.sort((a, b) => a - b);
    }

    return {
      timezones,
      myTimezone,
      offsetsInMinutes,
      myOffsetInMinutes,
    };
  }, []);

  return [
    <MenuDivider key="type" title="Timezone type" />,
    <MenuItem
      key="default"
      icon={tickIcon(!sqlTimeZone)}
      text="Default"
      label={defaultSqlTimeZone}
      shouldDismissPopover={false}
      onClick={() => setSqlTimeZone(undefined)}
    />,
    <MenuItem key="named" icon={tickIcon(String(sqlTimeZone).includes('/'))} text="Named">
      {timezones.map(({ timezone, offsetInMinutes }) => (
        <MenuItem
          key={timezone}
          icon={tickIcon(timezone === sqlTimeZone)}
          text={
            timezone === myTimezone ? (
              <>
                {timezone} <Icon icon={IconNames.STAR} data-tooltip="Browser timezone" />
              </>
            ) : (
              timezone
            )
          }
          label={`UTC${timezoneOffsetInMinutesToString(offsetInMinutes, false)}`}
          shouldDismissPopover={false}
          onClick={() => setSqlTimeZone(timezone)}
        />
      ))}
    </MenuItem>,
    <MenuItem key="offset" icon={tickIcon(String(sqlTimeZone).includes(':'))} text="Offset">
      {offsetsInMinutes.map(offsetInMinutes => {
        const offset = timezoneOffsetInMinutesToString(offsetInMinutes, true);
        return (
          <MenuItem
            key={offset}
            icon={tickIcon(offset === sqlTimeZone)}
            text={
              myOffsetInMinutes === offsetInMinutes ? (
                <>
                  {offset} <Icon icon={IconNames.STAR} data-tooltip="Browser offset" />
                </>
              ) : (
                offset
              )
            }
            shouldDismissPopover={false}
            onClick={() => setSqlTimeZone(offset)}
          />
        );
      })}
    </MenuItem>,
  ];
};
