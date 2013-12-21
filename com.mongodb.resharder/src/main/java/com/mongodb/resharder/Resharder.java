package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

public class Resharder implements Runnable {
	private final MongoClient _srcClient, _tgtClient, _logClient;

	private String[] params;

	private static DBCollection _source, _target, _oplogOut;

	public static DBCollection getSource() {
		return _source;
	}

	public static DBCollection getTarget() {
		return _target;
	}

	public Resharder() throws UnknownHostException {
		_srcClient = new MongoClient(new MongoClientURI(Config.get_srcURI()));
		_tgtClient = new MongoClient(new MongoClientURI(Config.get_tgtURI()));
		_logClient = new MongoClient(new MongoClientURI(Config.getLogURI()));

		Config.set_log(_logClient.getDB("resharder").getCollection("log"));
		Config.set_adminDB((_srcClient.getDB("admin")));

		params = Config.get_TargetNamepace().split("\\.");
		Config.set_tgtCollection(_tgtClient.getDB(params[0]).getCollection(params[1]));
		Config.get_tgtCollection().drop();
		_oplogOut = _tgtClient.getDB("resharder").getCollection("oplog_out");
		_oplogOut.drop();
	}

	public void run() {
		try {
			// Shutting down balancer...does this happen immediately and
			// what happens if a chunk migration is in progress?
			CommandResult result = Config.get_adminDB().doEval(
					"function() {sh.setBalancerState(false); return sh.getBalancerState();}", new Object[0]);

			// TODO cleanup any orphan documents before proceeding

			List<Shard> shards = ShardMapper.getShards();

			// Start a reader for each shard
			MessageLog.push("Processing shard config...", "Launcher");
			for (Shard shard : shards) {
				MongoClient mongo = new MongoClient(shard.hosts());

				// TODO check for secondary read flag and if set only read oplog
				// from primary
				// Get a connection to the shard Primary
				params = Config.get_Namepace().split("\\.");
				DBCollection source = mongo.getDB(params[0]).getCollection(params[1]);
				DBCollection oplog = mongo.getDB("local").getCollection("oplog.rs");

				OpLogReader olr = new OpLogReader(oplog, _oplogOut, Config.get_Namepace());
				
				new Thread(olr).start();
				
				CollectionScanner cs = new CollectionScanner(source, Config.get_readBatch());

				new Thread(cs).start();
			}

			// Start the writer thread
			// TODO - determine if one writer per reader is needed
			DocWriter dw = new DocWriter(Config.get_tgtCollection(), Config.get_writeBatch());
			new Thread(dw).start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
