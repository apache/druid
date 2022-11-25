./.github/scripts/setup_generate_license.sh
${MVN} apache-rat:check -Prat --fail-at-end \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
-Drat.consoleOutput=true ${HADOOP_PROFILE}
# Generate dependency reports and checks they are valid.
mkdir -p target
distribution/bin/generate-license-dependency-reports.py . target --clean-maven-artifact-transfer --parallel 2
distribution/bin/check-licenses.py licenses.yaml target/license-reports