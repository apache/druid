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

import unittest
import check_test_suite

class CheckTestSuite(unittest.TestCase):
    def test_always_run(self):
        for job in check_test_suite.always_run_jobs:
            self.assertEqual(True, check_test_suite.check_should_run_suite(job, ['.travis.yml']))
            self.assertEqual(True, check_test_suite.check_should_run_suite(job, ['docs/ingestion/index.md']))
            self.assertEqual(True, check_test_suite.check_should_run_suite(job, ['web-console/src/views/index.ts']))
            self.assertEqual(True, check_test_suite.check_should_run_suite(job, ['core/src/main/java/org/apache/druid/math/expr/Expr.java']))

    def test_docs(self):
        self.assertEqual(False, check_test_suite.check_docs('.travis.yml'))
        self.assertEqual(False, check_test_suite.check_docs('check_test_suite_test.py'))
        self.assertEqual(True, check_test_suite.check_docs('website/core/Footer.js'))
        self.assertEqual(True, check_test_suite.check_docs('docs/ingestion/index.md'))

        for job in check_test_suite.docs_jobs:
            self.assertEqual(
                True,
                check_test_suite.check_should_run_suite(job, ['check_test_suite_test.py', 'docs/ingestion/index.md'])
            )
            self.assertEqual(
                False,
                check_test_suite.check_should_run_suite(
                    job,
                    ['check_test_suite_test.py', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
                )
            )

    def test_web_console(self):
        web_console_job = 'web console'
        e2e_job = 'web console end-to-end test'
        self.assertEqual(False, check_test_suite.check_console('.travis.yml'))
        self.assertEqual(False, check_test_suite.check_console('check_test_suite_test.py'))
        self.assertEqual(False, check_test_suite.check_console('website/core/Footer.js'))
        self.assertEqual(True, check_test_suite.check_console('web-console/assets/azure.png'))
        self.assertEqual(True, check_test_suite.check_console('web-console/src/views/index.ts'))
        self.assertEqual(True, check_test_suite.check_console('web-console/unified-console.html'))

        self.assertEqual(
            True,
            check_test_suite.check_should_run_suite(
                web_console_job,
                ['check_test_suite_test.py', 'web-console/unified-console.html']
            )
        )
        self.assertEqual(
            False,
            check_test_suite.check_should_run_suite(
                web_console_job,
                ['check_test_suite_test.py', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
            )
        )
        self.assertEqual(
            True,
            check_test_suite.check_should_run_suite(
                e2e_job,
                ['check_test_suite_test.py', 'web-console/unified-console.html']
            )
        )
        self.assertEqual(
            True,
            check_test_suite.check_should_run_suite(
                e2e_job,
                ['check_test_suite_test.py', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
            )
        )

    def test_testable_script(self):
        self.assertEqual(False, check_test_suite.check_testable_script('.travis.yml'))
        self.assertEqual(True, check_test_suite.check_testable_script('check_test_suite.py'))
        self.assertEqual(True, check_test_suite.check_testable_script('check_test_suite_test.py'))

        script_job = 'script checks'
        some_java_job = 'spotbugs checks'
        self.assertEqual(
            False,
            check_test_suite.check_should_run_suite(
                script_job,
                ['core/src/main/java/org/apache/druid/math/expr/Expr.java']
            )
        )
        self.assertEqual(
            True,
            check_test_suite.check_should_run_suite(
                some_java_job,
                ['check_test_suite_test.py', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
            )
        )
        self.assertEqual(
            True,
            check_test_suite.check_should_run_suite(
                some_java_job,
                ['check_test_suite_test.py', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
            )
        )
        self.assertEqual(
            False,
            check_test_suite.check_should_run_suite(
                some_java_job,
                ['check_test_suite_test.py']
            )
        )

    def test_some_java(self):

        some_java_job = 'spotbugs checks'
        some_non_java_diffs = [
            ['.travis.yml'],
            ['check_test_suite_test.py'],
            ['website/core/Footer.js'],
            ['web-console/src/views/index.ts'],
            ['licenses/foo.mit'],
            ['check_test_suite_test.py', 'website/core/Footer.js', 'web-console/unified-console.html', 'owasp-dependency-check-suppressions.xml']
        ]
        some_java_diffs = [
            ['core/src/main/java/org/apache/druid/math/expr/Expr.java'],
            ['processing/src/main/java/org/apache/druid/segment/virtual/ExpressionPlan.java'],
            ['check_test_suite_test.py', 'website/core/Footer.js', 'web-console/unified-console.html', 'core/src/main/java/org/apache/druid/math/expr/Expr.java']
        ]

        for false_diff in some_non_java_diffs:
            self.assertEqual(
                False,
                check_test_suite.check_should_run_suite(
                    some_java_job,
                    false_diff
                )
            )
        for true_diff in some_java_diffs:
            self.assertEqual(
                True,
                check_test_suite.check_should_run_suite(
                    some_java_job,
                    true_diff
                )
            )


if __name__ == '__main__':
    unittest.main()
