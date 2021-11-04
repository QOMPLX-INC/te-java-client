#!/bin/bash

# Run tests using keycloak credentials. Usage:
# * run using access token for authorization
#   $ run_test_keycloak.sh "access token"
# * run using keycloack server url and client credentials for autorization
#   $ run_test_keycloak.sh "keycloak server auth url" "client_id" "client_secret"
# For example:
#   $ run_test_keycloak.sh "http://your.server/auth/realms/yourrealm/protocol/openid-connect/token" "my-app" "my-secret"

JAVA_PROGRAM_ARGS=`echo "$@"`
echo $JAVA_PROGRAM_ARGS
mvn exec:java -Dexec.mainClass="examples.com.qomplx.mdtsdb.client.Test" -Dexec.args="$JAVA_PROGRAM_ARGS"