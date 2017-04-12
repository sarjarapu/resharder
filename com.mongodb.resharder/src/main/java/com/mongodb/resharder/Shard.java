package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class Shard {
	private String _name, _srcHost;
	private List<ServerAddress> _hosts;
	private boolean _isreplset;
	private DBCollection _oplog, _source;
	private OpLogReader _olr;

	public Shard(String name, List<ServerAddress> hosts, boolean isreplset) {
		this._name = name;
		this._hosts = hosts;
		this._isreplset = isreplset;
	}

	public void setReplicationDelay(int seconds) throws InterruptedException {
		DBObject conf = (DBObject) _oplog.getDB().getMongo().getDB("admin")
				.doEval("function() {return rs.conf();}", new Object[0]).get("retval");
		BasicDBList list = (BasicDBList) conf.get("members");
		for (int i = 0; i < list.size(); i++) {
			DBObject obj = (DBObject) list.get(i);

			if (_srcHost.equals(obj.get("host"))) {
				try {
					_oplog.getDB()
							.getMongo()
							.getDB("admin")
							.doEval("function() {cfg = rs.conf(); cfg.members[" + i + "].priority = "
									+ (seconds > 0 ? 0 : 1) + "; cfg.members[" + i + "].hidden = "
									+ (seconds > 0 ? "true" : "false") + "; cfg.members[" + i + "].slaveDelay = "
									+ seconds + "; rs.reconfig(cfg)}", new Object[0]);
				} catch (Exception e) {
					// connection reset not unexpected when
					// executing rs.reconfig()
					// let the reconfig finish
					Thread.sleep(2000);

					if (seconds > 0) {
						MessageLog.push("Replication delay set to " + seconds / 3600 + " hours on " + _srcHost, this
								.getClass().getSimpleName());
					} else {
						MessageLog.push("Replication delay set to 0 on " + _srcHost, this.getClass().getSimpleName());
					}
				}
			}
		}
	}

	public void initReaders() throws InterruptedException, UnknownHostException {
		// get the oplog client
		MongoClient oplogClient = new MongoClient(_hosts), dataClient = oplogClient;

		// we need to hold a connection open to read the currently connected
		// host
		//oplogClient.getDB("admin").requestStart();
		//oplogClient.getDB("admin").requestEnsureConnection();

		String target = oplogClient.getAddress().getHost() + ":" + oplogClient.getAddress().getPort();
		target = Config.get_nodes().findOne(new BasicDBObject("host", target)).get("name").toString();

		new Node(Config.get_nodes().findOne(new BasicDBObject("name", "mongos"))).addConnection(target, "");

		//oplogClient.getDB("admin").requestDone();

		// Get a pointer to the shard Primary oplog.rs
		_oplog = oplogClient.getDB("local").getCollection("oplog.rs");

		_olr = new OpLogReader(_oplog);

		if (Config.is_readSecondary()) {
			// get a list of replica set members
			DBObject retval = (DBObject) oplogClient.getDB("admin")
					.doEval("function() {return rs.isMaster();}", new Object[0]).get("retval");

			// find a secondary
			for (Object host : (BasicDBList) retval.get("hosts")) {
				if (!host.equals(retval.get("me"))) {
					_srcHost = host.toString();

					MongoClientURI uri = new MongoClientURI("mongodb://" + host);
					dataClient = new MongoClient(uri);
					dataClient.setReadPreference(ReadPreference.secondary());

					// get last oplog.rs timestamp
					DBCursor cursor = dataClient.getDB("local").getCollection("oplog.rs").find()
							.sort(new BasicDBObject("ts", -1)).limit(1);

					try {
						if (cursor.hasNext()) {
							DBObject doc = cursor.next();
							_olr.setTimestamp((BSONTimestamp) doc.get("ts"));
						} else
							_olr.setTimestamp(new BSONTimestamp());
					} finally {
						cursor.close();
					}

					// set replication delay
					this.setReplicationDelay(36000);

					break;
				}
			}
		}

		// Get a pointer to the source Collection
		_source = dataClient.getDB(Config.get_Namepace().split("\\.")[0]).getCollection(
				Config.get_Namepace().split("\\.")[1]);

		// add the connection for the UI data, TODO - make sure we have to open
		// this connection to populate the host/port values
		//dataClient.getDB("admin").requestStart();
		//dataClient.getDB("admin").requestEnsureConnection();

		target = dataClient.getAddress().getHost() + ":" + dataClient.getAddress().getPort();
		target = Config.get_nodes().findOne(new BasicDBObject("host", target)).get("name").toString();

		//dataClient.getDB("admin").requestDone();

		String key = Config.get_Namepace() + "." + _name;
		Chunk[] chunks = Config.get_chunks().get(key).toArray(new Chunk[0]);
		Arrays.sort(chunks);
		Chunk root = chunks[chunks.length / 2].load(chunks, 0, chunks.length - 1);

		// add the oplog reader
		Resharder.addWorker(_olr);

		// create and add collection scanners
		MessageLog.push("Starting " + Config.get_numReaders() + " reader(s)", this.getClass().getSimpleName());

		int start = 0, stop = Integer.parseInt(Long.toString(_source.count()));
		for (int i = 0; i < Config.get_numReaders(); i++) {

			start = Integer.parseInt(Long.toString(_source.count())) / Config.get_numReaders() * i;
			
			if (i + 1 == Config.get_numReaders())
				stop = Integer.parseInt(Long.toString(_source.count()));
			else
				stop = Integer.parseInt(Long.toString(_source.count())) / Config.get_numReaders() * (i + 1);

			Resharder.addWorker(new CollectionScanner(_source, root, start, stop));
		}
	}

	public String getName() {
		return _name;
	}

	public List<ServerAddress> hosts() {
		return _hosts;
	}

	public boolean isReplSet() {
		return _isreplset;
	}
}
