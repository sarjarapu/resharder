package com.mongodb.resharder;

import java.io.EOFException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class Resharder implements Runnable {

	private String _DBname, _collectionName;

	public Resharder() {
		String[] params = Conf.get_Namepace().split("\\.");
		_DBname = params[0];
		_collectionName = params[1];
	}

	public void run() {
		try {
			CommandResult result;

			// Tell balancer not to run
			result = Conf.get_adminDB().doEval(
					"function() {sh.setBalancerState(false); return sh.getBalancerState();}", new Object[0]);

			if (Boolean.parseBoolean(result.toString())) {
				MessageLog.push("Unable to turn off Balancer", this.getClass().getSimpleName());
			}

			// Wait for balancer to stop if active
			DBCursor balancer;
			while (true) {
				balancer = Conf.get_configDB().getCollection("locks")
						.find(new BasicDBObject("_id", "balancer").append("state", "2")).limit(1);
				try {
					if (!balancer.hasNext())
						break;

					MessageLog.push("Balancer is active, waiting for lock release...", this.getClass().getSimpleName());
					Thread.sleep(10000);
				} finally {
					balancer.close();
				}
			}

			MessageLog.push("Balancer is inactive and disabled.", this.getClass().getSimpleName());

			// Shard the target collection if reshard is true
			if (Conf.is_reshard()) {
				result = Conf.get_adminDB().command(
						new BasicDBObject("enableSharding", Conf.get_TargetNamepace().split("\\.")[0]));

				if (result.getInt("ok") != 1 && !result.getString("errmsg").equals("already enabled")) {
					MessageLog.push("Unable to enable sharding on target DB.  errmsg: " + result.getString("errmsg"),
							this.getClass().getSimpleName());
					return;
				}

				result = Conf.get_adminDB().command(
						new BasicDBObject("shardCollection", Conf.get_TargetNamepace()).append("key",
								new BasicDBObject(Conf.get_reshardKey(), 1)));

				if (result.getInt("ok") != 1 && !result.getString("errmsg").equals("already sharded")) {
					MessageLog.push(
							"Unable to enable sharding on target Collection.  errmsg: " + result.getString("errmsg"),
							this.getClass().getSimpleName());
					return;
				}

				MessageLog.push("Target collection configured for sharding.  key: " + Conf.get_reshardKey(), this
						.getClass().getSimpleName());
			}

			ShardMapper.getShardingStatus(null);
			List<Shard> shards = ShardMapper.getShards();

			// Start the rate monitor
			Launcher._tp.schedule(new PerfCounters(), 0, TimeUnit.MILLISECONDS);

			// Start a reader for each shard
			MessageLog.push("Processing shard config...", "Launcher");
			for (Shard shard : shards) {
				// get the oplog client
				MongoClient oplogClient = new MongoClient(shard.hosts()), dataClient = oplogClient;

				// Get a pointer to the shard Primary oplog.rs
				DBCollection oplog = oplogClient.getDB("local").getCollection("oplog.rs");
				OpLogReader olr = new OpLogReader(oplog);

				if (Conf.is_readSecondary()) {
					DBObject retval = (DBObject) oplogClient.getDB("admin")
							.doEval("function() {return rs.isMaster();}", new Object[0]).get("retval");

					for (Object host : (BasicDBList) retval.get("hosts")) {
						if (!host.equals(retval.get("me"))) {
							MongoClientURI uri = new MongoClientURI("mongodb://" + host);
							dataClient = new MongoClient(uri);
							dataClient.setReadPreference(ReadPreference.secondary());

							// set last oplog.rs timestamp
							DBCursor cursor = dataClient.getDB("local").getCollection("oplog.rs").find()
									.sort(new BasicDBObject("ts", -1)).limit(1);

							try {
								if (cursor.hasNext()) {
									DBObject doc = cursor.next();
									olr.setTimestamp((BSONTimestamp) doc.get("ts"));
								} else
									olr.setTimestamp(new BSONTimestamp());
							} finally {
								cursor.close();
							}

							// set replication delay
							DBObject conf = (DBObject) oplogClient.getDB("admin")
									.doEval("function() {return rs.conf();}", new Object[0]).get("retval");
							BasicDBList list = (BasicDBList) conf.get("members");
							for (int i = 0; i < list.size(); i++) {
								DBObject obj = (DBObject) list.get(i);

								if (host.equals(obj.get("host"))) {
									try {
										result = oplogClient.getDB("admin").doEval(
												"function() {cfg = rs.conf(); cfg.members[" + i
														+ "].priority = 0; cfg.members[" + i
														+ "].hidden = true; cfg.members[" + i
														+ "].slaveDelay = 36000; rs.reconfig(cfg)}", new Object[0]);
									} catch (Exception e) {
										MessageLog.push("Replication delay set to 10 hours on " + host, this.getClass().getSimpleName());
										// NOOP - connection reset not unexpected when executing rs.reconfig()
									}
								}
							}

							break;
						}
					}
				}

				// Get a pointer to the source Collection
				DBCollection source = dataClient.getDB(_DBname).getCollection(_collectionName);

				String key = Conf.get_Namepace() + "." + shard.getName();
				Chunk[] chunks = Conf.get_chunks().get(key).toArray(new Chunk[0]);
				Arrays.sort(chunks);
				Chunk root = chunks[chunks.length / 2].load(chunks, 0, chunks.length - 1);

				// Start the oplog reader
				Launcher._tp.schedule(olr, 0, TimeUnit.MILLISECONDS);

				// Start the collection scanner
				CollectionScanner cs = new CollectionScanner(source, root);
				Launcher._tp.schedule(cs, 0, TimeUnit.MILLISECONDS);
			}

			// Start the writer thread
			// TODO - determine if one writer per reader is needed

			DocWriter dw = new DocWriter();
			Launcher._tp.schedule(dw, 1, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
