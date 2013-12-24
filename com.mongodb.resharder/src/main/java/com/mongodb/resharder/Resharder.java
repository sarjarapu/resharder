package com.mongodb.resharder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class Resharder implements Runnable {

	private String[] params;

	public Resharder() {
		params = Config.get_TargetNamepace().split("\\.");
	}

	public void run() {
		try {
			// Tell balancer not to run
			Config.get_adminDB().doEval(
					"function() {sh.setBalancerState(false); return sh.getBalancerState();}", new Object[0]);

			MessageLog.push("Balancer state set to false.", this.getClass().getSimpleName());
			
			
			// Wait for balancer to stop if active
			DBCursor balancer;
			while (true) {
				balancer = Config.get_configDB().getCollection("locks")
						.find(new BasicDBObject("_id", "balancer").append("state", "2")).limit(1);
				try {
					if (!balancer.hasNext())
						break;
					
					MessageLog.push("Balancer is active, polling for lock release...", this.getClass().getSimpleName());
					Thread.sleep(10000);
				} finally {
					balancer.close();
				}
			}

			ShardMapper.getShardingStatus(null);
			List<Shard> shards = ShardMapper.getShards();

			// Start a reader for each shard
			MessageLog.push("Processing shard config...", "Launcher");
			for (Shard shard : shards) {
				MongoClient mongo = new MongoClient(shard.hosts());

				// TODO check for secondary read and if true only read oplog
				// from primary
				
				// Get a connection to the shard Primary
				params = Config.get_Namepace().split("\\.");
				DBCollection source = mongo.getDB(params[0]).getCollection(params[1]);
				DBCollection oplog = mongo.getDB("local").getCollection("oplog.rs");

				OpLogReader olr = new OpLogReader(oplog);
				Launcher._tp.schedule(olr, 0, TimeUnit.MILLISECONDS);

				String key = Config.get_Namepace() + "." + shard.getName();
				Chunk[] chunks = Config.get_chunks().get(key).toArray(new Chunk[0]);
				Chunk root = chunks[chunks.length / 2].load(chunks, 0, chunks.length - 1);
				
				CollectionScanner cs = new CollectionScanner(source, Config.get_readBatch(), root);
				Launcher._tp.schedule(cs, 0, TimeUnit.MILLISECONDS);
			}

			// Start the writer thread
			// TODO - determine if one writer per reader is needed
			DocWriter dw = new DocWriter(Config.get_tgtCollection(), Config.get_writeBatch());
			Launcher._tp.schedule(dw, 0, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
