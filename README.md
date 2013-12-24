resharder
=========

Java utility for cloning sharded MongoDB collections with a new Shard Key

To create a local test environment unzip test.zip, cd to the test/bin directory and execute ./shard.sh from the command line.  This script will kill any mongod instances running on the box so don't run it on a production server.

To test Resharder compile the project and execute the jar or run within an IDE such as IntelliJ or Eclipse.  If you have a mongos instance running on localhost:27017 or have initialized the test environment Resharder will pull shard configuration automatically.  I will add support for remote mongos when I get a chance.
