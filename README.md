jclouds-vcloud-director
=======================

In order to release a new version:
1. create a new `release` branch from `1.8.x` branch
2. update the version inside `release/new-branch` pom.xml
3. mvn clean install
If everything is ok, finally push it to artifactory
4. mvn deploy -DaltDeploymentRepository=cloudsoft-deploy-artifactory-release::default::http://ccweb.cloudsoftcorp.com/maven/libs-release-local/ -DskipTests