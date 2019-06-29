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

import yaml
import json
import os
import sys
from html.parser import HTMLParser


apache_license_v2 = "\n\
                                 Apache License\n\
                           Version 2.0, January 2004\n\
                        http://www.apache.org/licenses/bin/\n\
\n\
   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION\n\
\n\
   1. Definitions.\n\
\n\
      \"License\" shall mean the terms and conditions for use, reproduction,\n\
      and distribution as defined by Sections 1 through 9 of this document.\n\
\n\
      \"Licensor\" shall mean the copyright owner or entity authorized by\n\
      the copyright owner that is granting the License.\n\
\n\
      \"Legal Entity\" shall mean the union of the acting entity and all\n\
      other entities that control, are controlled by, or are under common\n\
      control with that entity. For the purposes of this definition,\n\
      \"control\" means (i) the power, direct or indirect, to cause the\n\
      direction or management of such entity, whether by contract or\n\
      otherwise, or (ii) ownership of fifty percent (50%) or more of the\n\
      outstanding shares, or (iii) beneficial ownership of such entity.\n\
\n\
      \"You\" (or \"Your\") shall mean an individual or Legal Entity\n\
      exercising permissions granted by this License.\n\
\n\
      \"Source\" form shall mean the preferred form for making modifications,\n\
      including but not limited to software source code, documentation\n\
      source, and configuration files.\n\
\n\
      \"Object\" form shall mean any form resulting from mechanical\n\
      transformation or translation of a Source form, including but\n\
      not limited to compiled object code, generated documentation,\n\
      and conversions to other media types.\n\
\n\
      \"Work\" shall mean the work of authorship, whether in Source or\n\
      Object form, made available under the License, as indicated by a\n\
      copyright notice that is included in or attached to the work\n\
      (an example is provided in the Appendix below).\n\
\n\
      \"Derivative Works\" shall mean any work, whether in Source or Object\n\
      form, that is based on (or derived from) the Work and for which the\n\
      editorial revisions, annotations, elaborations, or other modifications\n\
      represent, as a whole, an original work of authorship. For the purposes\n\
      of this License, Derivative Works shall not include works that remain\n\
      separable from, or merely link (or bind by name) to the interfaces of,\n\
      the Work and Derivative Works thereof.\n\
\n\
      \"Contribution\" shall mean any work of authorship, including\n\
      the original version of the Work and any modifications or additions\n\
      to that Work or Derivative Works thereof, that is intentionally\n\
      submitted to Licensor for inclusion in the Work by the copyright owner\n\
      or by an individual or Legal Entity authorized to submit on behalf of\n\
      the copyright owner. For the purposes of this definition, \"submitted\"\n\
      means any form of electronic, verbal, or written communication sent\n\
      to the Licensor or its representatives, including but not limited to\n\
      communication on electronic mailing lists, source code control systems,\n\
      and issue tracking systems that are managed by, or on behalf of, the\n\
      Licensor for the purpose of discussing and improving the Work, but\n\
      excluding communication that is conspicuously marked or otherwise\n\
      designated in writing by the copyright owner as \"Not a Contribution.\"\n\
\n\
      \"Contributor\" shall mean Licensor and any individual or Legal Entity\n\
      on behalf of whom a Contribution has been received by Licensor and\n\
      subsequently incorporated within the Work.\n\
\n\
   2. Grant of Copyright License. Subject to the terms and conditions of\n\
      this License, each Contributor hereby grants to You a perpetual,\n\
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable\n\
      copyright license to reproduce, prepare Derivative Works of,\n\
      publicly display, publicly perform, sublicense, and distribute the\n\
      Work and such Derivative Works in Source or Object form.\n\
\n\
   3. Grant of Patent License. Subject to the terms and conditions of\n\
      this License, each Contributor hereby grants to You a perpetual,\n\
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable\n\
      (except as stated in this section) patent license to make, have made,\n\
      use, offer to sell, sell, import, and otherwise transfer the Work,\n\
      where such license applies only to those patent claims licensable\n\
      by such Contributor that are necessarily infringed by their\n\
      Contribution(s) alone or by combination of their Contribution(s)\n\
      with the Work to which such Contribution(s) was submitted. If You\n\
      institute patent litigation against any entity (including a\n\
      cross-claim or counterclaim in a lawsuit) alleging that the Work\n\
      or a Contribution incorporated within the Work constitutes direct\n\
      or contributory patent infringement, then any patent licenses\n\
      granted to You under this License for that Work shall terminate\n\
      as of the date such litigation is filed.\n\
\n\
   4. Redistribution. You may reproduce and distribute copies of the\n\
      Work or Derivative Works thereof in any medium, with or without\n\
      modifications, and in Source or Object form, provided that You\n\
      meet the following conditions:\n\
\n\
      (a) You must give any other recipients of the Work or\n\
          Derivative Works a copy of this License; and\n\
\n\
      (b) You must cause any modified files to carry prominent notices\n\
          stating that You changed the files; and\n\
\n\
      (c) You must retain, in the Source form of any Derivative Works\n\
          that You distribute, all copyright, patent, trademark, and\n\
          attribution notices from the Source form of the Work,\n\
          excluding those notices that do not pertain to any part of\n\
          the Derivative Works; and\n\
\n\
      (d) If the Work includes a \"NOTICE\" text file as part of its\n\
          distribution, then any Derivative Works that You distribute must\n\
          include a readable copy of the attribution notices contained\n\
          within such NOTICE file, excluding those notices that do not\n\
          pertain to any part of the Derivative Works, in at least one\n\
          of the following places: within a NOTICE text file distributed\n\
          as part of the Derivative Works; within the Source form or\n\
          documentation, if provided along with the Derivative Works; or,\n\
          within a display generated by the Derivative Works, if and\n\
          wherever such third-party notices normally appear. The contents\n\
          of the NOTICE file are for informational purposes only and\n\
          do not modify the License. You may add Your own attribution\n\
          notices within Derivative Works that You distribute, alongside\n\
          or as an addendum to the NOTICE text from the Work, provided\n\
          that such additional attribution notices cannot be construed\n\
          as modifying the License.\n\
\n\
      You may add Your own copyright statement to Your modifications and\n\
      may provide additional or different license terms and conditions\n\
      for use, reproduction, or distribution of Your modifications, or\n\
      for any such Derivative Works as a whole, provided Your use,\n\
      reproduction, and distribution of the Work otherwise complies with\n\
      the conditions stated in this License.\n\
\n\
   5. Submission of Contributions. Unless You explicitly state otherwise,\n\
      any Contribution intentionally submitted for inclusion in the Work\n\
      by You to the Licensor shall be under the terms and conditions of\n\
      this License, without any additional terms or conditions.\n\
      Notwithstanding the above, nothing herein shall supersede or modify\n\
      the terms of any separate license agreement you may have executed\n\
      with Licensor regarding such Contributions.\n\
\n\
   6. Trademarks. This License does not grant permission to use the trade\n\
      names, trademarks, service marks, or product names of the Licensor,\n\
      except as required for reasonable and customary use in describing the\n\
      origin of the Work and reproducing the content of the NOTICE file.\n\
\n\
   7. Disclaimer of Warranty. Unless required by applicable law or\n\
      agreed to in writing, Licensor provides the Work (and each\n\
      Contributor provides its Contributions) on an \"AS IS\" BASIS,\n\
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or\n\
      implied, including, without limitation, any warranties or conditions\n\
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A\n\
      PARTICULAR PURPOSE. You are solely responsible for determining the\n\
      appropriateness of using or redistributing the Work and assume any\n\
      risks associated with Your exercise of permissions under this License.\n\
\n\
   8. Limitation of Liability. In no event and under no legal theory,\n\
      whether in tort (including negligence), contract, or otherwise,\n\
      unless required by applicable law (such as deliberate and grossly\n\
      negligent acts) or agreed to in writing, shall any Contributor be\n\
      liable to You for damages, including any direct, indirect, special,\n\
      incidental, or consequential damages of any character arising as a\n\
      result of this License or out of the use or inability to use the\n\
      Work (including but not limited to damages for loss of goodwill,\n\
      work stoppage, computer failure or malfunction, or any and all\n\
      other commercial damages or losses), even if such Contributor\n\
      has been advised of the possibility of such damages.\n\
\n\
   9. Accepting Warranty or Additional Liability. While redistributing\n\
      the Work or Derivative Works thereof, You may choose to offer,\n\
      and charge a fee for, acceptance of support, warranty, indemnity,\n\
      or other liability obligations and/or rights consistent with this\n\
      License. However, in accepting such obligations, You may act only\n\
      on Your own behalf and on Your sole responsibility, not on behalf\n\
      of any other Contributor, and only if You agree to indemnify,\n\
      defend, and hold each Contributor harmless for any liability\n\
      incurred by, or claims asserted against, such Contributor by reason\n\
      of your accepting any such warranty or additional liability.\n\
\n\
   END OF TERMS AND CONDITIONS\n\
\n\
   APPENDIX: How to apply the Apache License to your work.\n\
\n\
      To apply the Apache License to your work, attach the following\n\
      boilerplate notice, with the fields enclosed by brackets \"[]\"\n\
      replaced with your own identifying information. (Don't include\n\
      the brackets!)  The text should be enclosed in the appropriate\n\
      comment syntax for the file format. We also recommend that a\n\
      file or class name and description of purpose be included on the\n\
      same \"printed page\" as the copyright notice for easier\n\
      identification within third-party archives.\n\
\n\
   Copyright [yyyy] [name of copyright owner]\n\
\n\
   Licensed under the Apache License, Version 2.0 (the \"License\");\n\
   you may not use this file except in compliance with the License.\n\
   You may obtain a copy of the License at\n\
\n\
       http://www.apache.org/licenses/bin/LICENSE-2.0\n\
\n\
   Unless required by applicable law or agreed to in writing, software\n\
   distributed under the License is distributed on an \"AS IS\" BASIS,\n\
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
   See the License for the specific language governing permissions and\n\
   limitations under the License.\n\
\n\
   APACHE DRUID (INCUBATING) SUBCOMPONENTS:\n\
\n\
   Apache Druid (incubating) includes a number of subcomponents with\n\
   separate copyright notices and license terms. Your use of the source\n\
   code for these subcomponents is subject to the terms and\n\
   conditions of the following licenses.\n\
\n"


class DependencyReportParser(HTMLParser):
    # TODO: Change to comments
    states = ["none", "h2_start", "project_dependencies_start", "project_dependencies_end", "h2_end", "h3_start", "compile_start", "compile_end", "h3_end", "table_start", "table_end", "row_start", "th_start", "th_end", "td_start", "td_end", "row_end"]
    attr_index = 0
    group_id = None
    artifact_id = None
    version = None
    classifier = None
    dep_type = None
    license = None
    state = "none"
    dep_to_license = None
    compatible_license_names = None
    include_classifier = False

    def __init__(self, compatible_license_names):
        HTMLParser.__init__(self)
        self.state = "none"
        self.compatible_license_names = compatible_license_names

    def parse(self, f):
        self.dep_to_license = {}
        self.feed(f.read())
        return self.dep_to_license

    def handle_starttag(self, tag, attrs):
        # print("current: {}, start tag: {}, attrs:{} ".format(self.state, tag, attrs))
        if self.state == "none":
            if tag == "h2":
                self.state = "h2_start"
        
        if self.state == "h2_start":
            if tag == "a":
                for attr in attrs:
                    if attr[0] == "name" and (attr[1] == "Project_Dependencies" or attr[1] == "Project_Transitive_Dependencies"):
                        self.state = "project_dependencies_start"
                        self.include_classifier = False

        if self.state == "h2_end":
            if tag == "h3":
                self.state = "h3_start"
        
        if self.state == "h3_start":
            if tag == "a":
                for attr in attrs:
                    if attr[0] == "name" and attr[1] == "compile":
                        self.state = "compile_start"
            
        if self.state == "h3_end":
            if tag == "table":
                self.state = "table_start"

        if self.state == "table_start":
            if tag == "tr":
                self.state = "row_start"
                self.clear_attr()

        if self.state == "row_end":
            if tag == "tr":
                self.state = "row_start"
                self.clear_attr()
        
        if self.state == "row_start":
            if tag == "td":
                self.state = "td_start"
            elif tag == "th":
                self.state = "th_start"

        if self.state == "th_end":
            if tag == "th":
                self.state = "th_start"
        
        if self.state == "td_end":
            if tag == "td":
                self.state = "td_start"

    def handle_endtag(self, tag):
        # print("current: {}, end tag: {}".format(self.state, tag))
        if self.state == "project_dependencies_start":
            if tag == "a":
                self.state = "project_dependencies_end"

        if self.state == "h2_start":
            if tag == "h2":
                self.state = "h2_end"
        
        if self.state == "project_dependencies_end":
            if tag == "h2":
                self.state = "h2_end"
        
        if self.state == "compile_start":
            if tag == "a":
                self.state = "compile_end"

        if self.state == "compile_end":
            if tag == "h3":
                self.state = "h3_end"
        
        if self.state == "table_start":
            if tag == "table":
                self.state = "none"

        if self.state == "td_start":
            if tag == "td":
                self.state = "td_end"
                self.attr_index = self.attr_index + 1
        
        if self.state == "th_start":
            if tag == "th":
                self.state = "th_end"

        if self.state == "row_start":
            if tag == "tr":
                self.state = "row_end"

        if self.state == "th_end":
            if tag == "tr":
                self.state = "row_end"
        
        if self.state == "td_end":
            if tag == "tr":
                self.state = "row_end"
                # print(json.dumps({"groupId": self.group_id, "artifactId": self.artifact_id, "version": self.version, "classifier": self.classifier, "type": self.dep_type, "license": self.license}))
                self.dep_to_license[get_dep_key(self.group_id, self.artifact_id, self.version)] = self.license
        
        if self.state == "row_end":
            if tag == "table":
                self.state = "none"

    def handle_data(self, data):
        if self.state == "td_start":
            self.set_attr(data)
        elif self.state == "th_start":
            if data.lower() == "classifier":
                self.include_classifier = True

    def clear_attr(self):
        self.group_id = None
        self.artifact_id = None
        self.version = None
        self.classifier = None
        self.dep_type = None
        self.license = None
        self.attr_index = 0

    def set_attr(self, data):
        #print("set data: {}".format(data))
        if self.attr_index == 0:
            self.group_id = data
        elif self.attr_index == 1:
            self.artifact_id = data
        elif self.attr_index == 2:
            self.version = data
        elif self.attr_index == 3:
            if self.include_classifier:
                self.classifier = data
            else:
                self.dep_type = data
        elif self.attr_index == 4:
            if self.include_classifier:
                self.dep_type = data
            else:
                self.set_license(data)
        elif self.attr_index == 5:
            if self.include_classifier:
                self.set_license(data)
            else:
                raise Exception("Unknown attr_index [{}]".format(self.attr_index))
        else:
            raise Exception("Unknown attr_index [{}]".format(self.attr_index))

    def set_license(self, data):
        if data.upper().find("GPL") < 0:
            if self.license != 'Apache License version 2.0':
                self.license = self.compatible_license_names[data]


def get_dep_key(group_id, artifact_id, version):
    return (group_id, artifact_id, version)


def build_compatible_license_names():
    compatible_licenses = {}
    compatible_licenses['Apache License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['The Apache Software License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache 2'] = 'Apache License version 2.0'
    compatible_licenses['Apache License 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache Software License - Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['The Apache License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License Version 2'] = 'Apache License version 2.0'
    compatible_licenses['Apache License v2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License, version 2.0'] = 'Apache License version 2.0'

    compatible_licenses['Public Domain'] = 'Public Domain'

    compatible_licenses['BSD-2-Clause License'] = 'BSD-2-Clause License'

    compatible_licenses['BSD-3-Clause License'] = 'BSD-3-Clause License'
    compatible_licenses['New BSD license'] = 'BSD-3-Clause License'
    compatible_licenses['BSD'] = 'BSD-3-Clause License'
    compatible_licenses['The BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD licence'] = 'BSD-3-Clause License'
    compatible_licenses['BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD-like'] = 'BSD-3-Clause License'
    compatible_licenses['The BSD 3-Clause License'] = 'BSD-3-Clause License'
    compatible_licenses['Revised BSD'] = 'BSD-3-Clause License'
    compatible_licenses['New BSD License'] = 'BSD-3-Clause License'

    compatible_licenses['ICU License'] = 'ICU License'

    compatible_licenses['SIL Open Font License 1.1'] = 'SIL Open Font License 1.1'

    compatible_licenses['CDDL 1.1'] = 'CDDL 1.1'
    compatible_licenses['CDDL/GPLv2+CE'] = 'CDDL 1.1'
    compatible_licenses['CDDL + GPLv2 with classpath exception'] = 'CDDL 1.1'
    compatible_licenses['CDDL License'] = 'CDDL 1.1'

    compatible_licenses['Eclipse Public License 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['The Eclipse Public License, Version 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License - Version 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License, Version 1.0'] = 'Eclipse Public License 1.0'

    compatible_licenses['Mozilla Public License Version 2.0'] = 'Mozilla Public License Version 2.0'
    compatible_licenses['Mozilla Public License, Version 2.0'] = 'Mozilla Public License Version 2.0'

    compatible_licenses['Creative Commons Attribution 2.5'] = 'Creative Commons Attribution 2.5'

    compatible_licenses['Creative Commons CC0'] = 'Creative Commons CC0'
    compatible_licenses['CC0'] = 'Creative Commons CC0'

    compatible_licenses['The MIT License'] = 'MIT License'
    compatible_licenses['MIT License'] = 'MIT License'

    compatible_licenses['-'] = '-'
    return compatible_licenses


def module_to_upper(module):
    extensions_offset = module.lower().find("extensions")
    if extensions_offset < 0:
        return module.upper()
    elif extensions_offset == 0:
        return module[0:len("extensions")].upper() + module[len("extensions"):len(module)]
    else:
        raise Exception("Expected extensions at 0, but {}".format(extensions_offset))


def print_error(str):
    print(str, file=sys.stderr)


def print_license_phrase(license_phrase):
    remaining = license_phrase
    while len(remaining) > 0:
        # print("remaining: {}".format(remaining))
        # print("len: {}".format(len(remaining)))
        if len(remaining) > 120:
            chars_of_200 = remaining[0:120]
            phrase_len = chars_of_200.rfind(" ")
            if phrase_len < 0:
                raise Exception("Can't find whitespace in {}".format(chars_of_200))
            print("    {}".format(remaining[0:phrase_len]))
            remaining = remaining[phrase_len:]
        else:
            print("    {}".format(remaining))
            remaining = ""


def is_non_empty(dic, key):
    if key in dic and dic[key] is not None:
        if type(dic[key]) == str:
            return len(dic[key]) > 0
        else:
            return True
    else:
        return False


def print_license(license):
    license_phrase = "This product"
    if license['license_category'] == "source":
        license_phrase += " contains"
    elif license['license_category'] == "binary":
        license_phrase += " bundles"
    license_phrase += " {}".format(license['name'])
    if is_non_empty(license, 'version'):
        license_phrase += " version {}".format(license['version'])
    if is_non_empty(license, 'copyright'):
        license_phrase += ", copyright {}".format(license['copyright'])
    if is_non_empty(license, 'additional_copyright_statement'):
        license_phrase += ", {}".format(license['additional_copyright_statement'])
    if license['license_name'] != 'Apache License version 2.0':
        license_phrase += " which is available under {}".format(license['license_name'])
    if is_non_empty(license, 'additional_license_statement'):
        license_phrase += ", {}".format(license['additional_license_statement'])
    if is_non_empty(license, 'license_file_path'):
        license_file_list = []
        if type(license['license_file_path']) == list:
            license_file_list.extend(license['license_file_path'])
        else:
            license_file_list.append(license['license_file_path'])
        if len(license_file_list) == 1:
            license_phrase += ". For details, see {}".format(license_file_list[0])
        else:
            license_phrase += ". For details, "
            for each_file in license_file_list:
                if each_file == license_file_list[-1]:
                    license_phrase += ", and {}".format(each_file)
                elif each_file == license_file_list[0]:
                    license_phrase += "see {}".format(each_file)
                else:
                    license_phrase += ", {}".format(each_file)
    
    license_phrase += "."

    print_license_phrase(license_phrase)

    if 'source_paths' in license:
        for source_path in license['source_paths']:
            if type(source_path) is dict:
                for class_name, path in source_path.items():
                    print("      {}:".format(class_name))
                    print("      * {}".format(path))
            else:
                print("      * {}".format(source_path))

    if 'libraries' in license:
        for library in license['libraries']:
            if type(library) is not dict:
                raise Exception("Expected dict but got {}[{}]".format(type(library), library))
            if len(library) > 1:
                raise Exception("Expected 1 groupId and artifactId, but got [{}]".format(library))
            for group_id, artifact_id in library.items():
                print("      * {}:{}".format(group_id, artifact_id))


def check_licenses(license_yaml, dependencie_reports_root):
    reported_dep_to_licenses = {}
    compatible_license_names = build_compatible_license_names()
    for dirpath, dirnames, filenames in os.walk(dependencie_reports_root):
        for filename in filenames:
            if filename == "dependencies.html":
                full_path = os.path.join(dirpath, filename)
                print_error("Parsing {}".format(full_path))
                with open(full_path) as report_file:
                    parser = DependencyReportParser(compatible_license_names)
                    reported_dep_to_licenses.update(parser.parse(report_file))
    
    print_error("Found {} reported licenses".format(len(reported_dep_to_licenses)))

    # Compare licenses in registry and those in dependency reports
    mismatched_licenses = []
    unchecked_licenses = []
    with open(license_yaml) as registry_file:
        licenses_list = list(yaml.load_all(registry_file))
        registered_dep_to_licenses = {}
        for license in licenses_list:
            if 'skip_dependency_report_check' in license and license['skip_dependency_report_check']:
                continue
            if 'libraries' in license:
                for library in license['libraries']:
                    if type(library) is not dict:
                        raise Exception("Expected dict but got {}[{}]".format(type(library), library))
                    if len(library) > 1:
                        raise Exception("Expected 1 groupId and artifactId, but got [{}]".format(library))
                    for group_id, artifact_id in library.items():
                        if 'version' not in license:
                            raise Exception("version is missing in {}".format(license))
                        if 'license_name' not in license:
                            raise Exception("name is missing in {}".format(license))
                        registered_dep_to_licenses[get_dep_key(group_id, artifact_id, license['version'])] = compatible_license_names[license['license_name']]

        for key, registered_license in registered_dep_to_licenses.items():
            if key in reported_dep_to_licenses:
                reported_license = reported_dep_to_licenses[key]
                if reported_license is not None and reported_license != "-" and reported_license != registered_license:
                    group_id = key[0]
                    artifact_id = key[1]
                    version = key[2]
                    mismatched_licenses.append((group_id, artifact_id, version, reported_license, registered_license))
        
        if len(mismatched_licenses) > 0:
            print_error("Error: found {} mismatches between reported licenses and registered licenses".format(len(mismatched_licenses)))
            for mismatched_license in mismatched_licenses:
                print_error("groupId: {}, artifactId: {}, version: {}, reported_license: {}, registered_license: {}".format(mismatched_license[0], mismatched_license[1], mismatched_license[2], mismatched_license[3], mismatched_license[4]))
            sys.exit(1)
        
        for key, registered_license in registered_dep_to_licenses.items():
            if key not in reported_dep_to_licenses:
                # print("{} is not in reported licenses[{}]".format(key, reported_dep_to_licenses))
                unchecked_licenses.append((key[0], key[1], key[2], registered_license))
            elif reported_dep_to_licenses[key] == "-":
                unchecked_licenses.append((key[0], key[1], key[2], registered_license))
        
        if len(unchecked_licenses) > 0:
            print_error("Warn: found {} unchecked licenses. These licenses are registered, but not found in dependency reports".format(len(unchecked_licenses)))
            for unchecked_license in unchecked_licenses:
                print_error("groupId: {}, artifactId: {}, version: {}, reported_license: {}".format(unchecked_license[0], unchecked_license[1], unchecked_license[2], unchecked_license[3]))
    print_error("")


def print_license_name_underbar(license_name):
    underbar = ""
    for _ in range(len(license_name)):
        underbar += "="
    print("{}\n".format(underbar))

def generate_license(license_yaml):
    # Generate LICENSE.BINARY file
    print_error("=== Generating the contents of LICENSE.BINARY file ===\n")
    
    print(apache_license_v2)
    with open(license_yaml) as registry_file:
        licenses_list = list(yaml.load_all(registry_file))
        licenses_map = {}
        for license in licenses_list:
            if license['license_name'] not in licenses_map:
                licenses_map[license['license_name']] = {}
            licenses_of_name = licenses_map[license['license_name']]
            if license['license_category'] not in licenses_of_name:
                licenses_of_name[license['license_category']] = {}
            licenses_of_category = licenses_of_name[license['license_category']]
            if license['module'] not in licenses_of_category:
                licenses_of_category[license['module']] = []
            licenses_of_module = licenses_of_category[license['module']]
            licenses_of_module.append(license)

        for license_name, licenses_of_name in sorted(licenses_map.items()):
            print(license_name)
            print_license_name_underbar(license_name)
            for license_category, licenses_of_category in licenses_of_name.items():
                for module, licenses in licenses_of_category.items():
                    print("{}/{}".format(license_category.upper(), module_to_upper(module)))
                    for license in licenses:
                        print_license(license)
                        print("")
                    print("")


# TODO: add options: fail-on-unchecked-license, debug mode
if len(sys.argv) != 3:
    sys.stderr.write("usage: {} <path to license.yaml> <root to maven dependency reports>".format(sys.argv[0]))
    sys.exit(1)

license_yaml = sys.argv[1]
dependencie_reports_root = sys.argv[2]

check_licenses(license_yaml, dependencie_reports_root)
generate_license(license_yaml)