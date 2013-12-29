package com.mongodb.resharder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class Resharder implements Runnable {

	private String[] params;

	public Resharder() {
		params = Conf.get_TargetNamepace().split("\\.");
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
				MongoClient mongo = new MongoClient(shard.hosts());

				// TODO check for secondary read and if true only read oplog
				// from primary

				// Get a connection to the shard Primary
				params = Conf.get_Namepace().split("\\.");
				DBCollection source = mongo.getDB(params[0]).getCollection(params[1]);
				DBCollection oplog = mongo.getDB("local").getCollection("oplog.rs");

				OpLogReader olr = new OpLogReader(oplog);
				Launcher._tp.schedule(olr, 0, TimeUnit.MILLISECONDS);

				String key = Conf.get_Namepace() + "." + shard.getName();
				Chunk[] chunks = Conf.get_chunks().get(key).toArray(new Chunk[0]);
				Arrays.sort(chunks);
				Chunk root = chunks[chunks.length / 2].load(chunks, 0, chunks.length - 1);

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
