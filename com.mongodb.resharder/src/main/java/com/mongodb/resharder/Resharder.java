package com.mongodb.resharder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import net.sf.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCursor;

public class Resharder implements Runnable {

	private static LinkedList<Runnable> _threads = new LinkedList<Runnable>();

	public static void addWorker(Runnable worker) {
		_threads.add(worker);
	}

	public static void shutdown() throws InterruptedException, IOException {

		OpLogReader.shutdown();
		OpLogWriter.shutdown();

		if (Config.is_readSecondary()) {
			for (Shard shard : ShardMapper.getShards()) {
				shard.setReplicationDelay(0);
			}

			// let the replica sets settle down
			Thread.sleep(2000);

			bounceMongos();
		}

		MessageLog.push("Clone/Reshard completed.  namespace: " + Config.get_TargetNamepace(),
				Resharder.class.getSimpleName());
	}

	public Resharder() {
		_threads = new LinkedList<Runnable>();
	}

	private static void bounceMongos() throws IOException, InterruptedException {
		// get the command line args
		CommandResult result = Config.get_adminDB().command("getCmdLineOpts");

		// build the command line and restart mongos on this host
		StringBuilder sb = new StringBuilder();
		BasicDBList list = (BasicDBList) result.get("argv");
		String[] argv = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			sb.append(list.get(i));
			sb.append(" ");
			argv[i] = list.get(i).toString();
		}

		// shutdown mongos
		MessageLog.push("Shutting down mongos...", Resharder.class.getSimpleName());

		try {
			result = Config.get_adminDB().command(new BasicDBObject("shutdown", 1));
		} catch (Exception e) {
			// NOOP we probably ended up here because the server went
			// away before we could get a result
		}

		MessageLog.push("Restarting mongos.  cmdLine: " + sb.toString(), Resharder.class.getSimpleName());
		Runtime.getRuntime().exec(argv);

		// let things settle down and initialize
		Thread.sleep(5000);

		// restart Balancer
		MessageLog.push("Restarting Balancer...", Resharder.class.getSimpleName());
		result = Config.get_adminDB().doEval("function() {sh.setBalancerState(true); return sh.getBalancerState();}",
				new Object[0]);

		if (Boolean.parseBoolean(result.toString())) {
			MessageLog.push("ERUnable to start Balancer", Resharder.class.getSimpleName());
		}
	}

	public void run() {
		try {
			CommandResult result;

			// Tell balancer not to run
			result = Config.get_adminDB().doEval(
					"function() {sh.setBalancerState(false); return sh.getBalancerState();}", new Object[0]);

			if (Boolean.parseBoolean(result.toString())) {
				MessageLog.push("Unable to turn off Balancer", this.getClass().getSimpleName());
			}

			// Wait for balancer to stop if active
			DBCursor balancer;
			while (true) {
				balancer = Config.get_configDB().getCollection("locks")
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
			if (Config.is_reshard()) {
				result = Config.get_adminDB().command(
						new BasicDBObject("enableSharding", Config.get_TargetNamepace().split("\\.")[0]));

				if (result.getInt("ok") != 1 && !result.getString("errmsg").equals("already enabled")) {
					MessageLog.push("Unable to enable sharding on target DB.  errmsg: " + result.getString("errmsg"),
							this.getClass().getSimpleName());
					return;
				}

				@SuppressWarnings("unchecked")
				Map<String, Object> map = JSONObject.fromObject(Config.get_reshardKey());

				result = Config.get_adminDB().command(
						new BasicDBObject("shardCollection", Config.get_TargetNamepace()).append("key",
								new BasicDBObject(map)));

				if (result.getInt("ok") != 1 && !result.getString("errmsg").equals("already sharded")) {
					MessageLog.push(
							"Unable to enable sharding on target Collection.  errmsg: " + result.getString("errmsg"),
							this.getClass().getSimpleName());
					return;
				}

				MessageLog.push("Target collection configured for sharding.  namespace: " + Config.get_TargetNamepace()
						+ "  key: " + Config.get_reshardKey(), this.getClass().getSimpleName());
			}

			ShardMapper.getShardingStatus(null);
			List<Shard> shards = ShardMapper.getShards();

			// make a list of Runnables we want to schedule
			_threads.add(new PerfCounters());

			// Start a reader for each shard
			MessageLog.push("Processing shard config...", "Launcher");
			for (Shard shard : shards) {
				shard.initReaders();
			}

			// TODO - determine if one writer per reader is needed
			// queue a writer thread
			_threads.add(new DocWriter());

			// if secondary read is true then restart mongos
			// as it is probably hosed from the rs.reconfig()
			if (Config.is_readSecondary()) {
				// let the replica sets settle down
				Thread.sleep(2000);

				bounceMongos();
			}

			// Start the threads
			ListIterator<Runnable> it = _threads.listIterator();
			while (it.hasNext()) {
				Runnable thread = it.next();
				Launcher._tp.execute(thread);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
