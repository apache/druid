${MVN} ${MAVEN_SKIP} dependency:analyze -DoutputXML=true -DignoreNonCompile=true -DfailOnWarning=true ${HADOOP_PROFILE} ||
{ echo "
    The dependency analysis has found a dependency that is either:

    1) Used and undeclared: These are available as a transitive dependency but should be explicitly
    added to the POM to ensure the dependency version. The XML to add the dependencies to the POM is
    shown above.

    2) Unused and declared: These are not needed and removing them from the POM will speed up the build
    and reduce the artifact size. The dependencies to remove are shown above.

    If there are false positive dependency analysis warnings, they can be suppressed:
    https://maven.apache.org/plugins/maven-dependency-plugin/analyze-mojo.html#usedDependencies
    https://maven.apache.org/plugins/maven-dependency-plugin/examples/exclude-dependencies-from-dependency-analysis.html

    For more information, refer to:
    https://maven.apache.org/plugins/maven-dependency-plugin/analyze-mojo.html

    " && false; }
