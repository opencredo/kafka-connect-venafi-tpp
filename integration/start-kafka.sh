#!/bin/bash -e

# connector start command.
exec "/opt/kafka/bin/connect-standalone.sh" "/opt/kafka/config/connect-standalone.properties" "/opt/kafka/config/venafi-source-connector.properties"