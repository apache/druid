#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import urllib.parse

def get_header_level(line):
    count = 0
    for element in line:
        if element == '#':
            count = count + 1
        else:
            break
    return count

def make_link_text(prefix, text):
    return "{}-{}".format(prefix, urllib.parse.quote_plus(text.lower().replace(' ', '-')))

def process_release_notes(release_version, release_notes, outfile):
    """
    rewrites markdown headers with an embedded html link so that github release notes can become linkable.

    parent header text is url-encoded and prefixed to the link text so that links are ensured to be unique, albeit a
    bit long.

    e.g.
        # New features
        ...
        ## Queries
        ...
        ### Some cool feature

    becomes:
        # <a name="0.22.0-new-features" href="#0.22.0.new-features">#</a> New features
        ...
        ## <a name="0.22.0-new-features-queries" href="...">#</a> Queries
        ...
        ### <a name="0.22.0-new-features-queries-some-cool-feature" href="...">#</a> Some cool feature

    Markdown headers which already have an embedded link of this form will be ignored (though this logic isn't very
    smart, if it starts with "<a href=" or like, any other valid link form it will be missed...)

    :param release_version: release version is always the first part of the embedded link prefix
    :param release_notes: markdown file that contains release notes
    :param outfile: destination for rewritten markdown file
    :return: nothing
    """
    with open(args.out_path, "w", encoding="utf-8") as outfile:
        with open(release_notes, encoding="utf-8") as file:
            current_level = 0
            current_prefix = release_version
            levels = []
            prefixes = []
            for line in file:
                header_level = get_header_level(line)
                header_text = line[header_level + 1:len(line) - 1]
                if (header_level > 0 and "<a name=" not in line):
                    if header_level > current_level:
                        levels.append(current_level)
                        prefixes.append(current_prefix)
                        current_level = header_level
                    elif header_level < current_level:
                        current_level = levels.pop()
                        prefixes.pop()
                        while (header_level < current_level):
                            current_level = levels.pop()
                            prefixes.pop()

                    current_prefix = prefixes.pop()
                    link_text = make_link_text(current_prefix, header_text)
                    prefixes.append(current_prefix)
                    current_prefix = link_text

                    print(
                        "{} <a name=\"{}\" href=\"#{}\">#</a> {}".format(
                            line[0:header_level],
                            link_text,
                            link_text,
                            line[header_level + 1:]
                        ),
                        file=outfile,
                        end = ''
                    )
                else:
                    print(line, file=outfile, end = '')
    return

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='rewrite markdown so that headings contain direct links')
        parser.add_argument('version', metavar='release version', type=str)
        parser.add_argument('release_notes', metavar='<path to release notes markdown file>', type=str)
        parser.add_argument('out_path', metavar='<path to output file>', type=str)
        args = parser.parse_args()

        process_release_notes(args.version, args.release_notes, args.out_path)

    except KeyboardInterrupt:
        print('Interrupted, closing.')
