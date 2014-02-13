resharder
=========

Java utility for cloning sharded MongoDB collections with a new Shard Key

To create a local test environment unzip test.zip, cd to the test/bin directory and execute ./shard.sh from the command line.  This script will kill any mongod instances running on the box so don't run it on a production server.

To run Resharder you can execute the following command from the test/bin directory:

./resharder \<congigServers\> \<logFile\>

The configServers parameter is the comma separated list of config servers for your sharded cluster.  If you are running the test environment this would be the following:

localhost:28040,localhost:28041,localhost:28042

The logFile is where you want Resharder to log mongos output.

If everything is setup correctly you should be able to point a browser at http://localhost:8082 and start experiencing the endless joy that can only come from completely destroying your data!

Resharder now has a nifty console interface!  If you prefer old school terminal style interaction you can blow apart your data using the resharder CLI by adding '--console' to the resharder command line.

EXAMPLE:

resharder localhost:28040,localhost:28041,localhost:28042 ./mongos.log --console

NOTE:  Resharder is currently fully functional, but still in development.  If you have questions feel free to email me at rick.houlihan@mongodb.com and I will try to respond.
