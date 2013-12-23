package com.mongodb.resharder;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import freemarker.template.SimpleHash;

public class Config {
	private static String _ns, _targetns, _key;
	private static DBCollection _src, _tgt, _log, _oplog;
	private static DB _adminDB;
	private static boolean _secondary = false, _reshard = false;
	private static int _readBatch, _writeBatch;

	public Config(String namespace, String targetns, int readBatch, int writeBatch, boolean reshard, String key,
			boolean secondary, String srchost, String tgthost, String loghost) throws UnknownHostException {
		MongoClient srcClient = new MongoClient(new MongoClientURI("mongodb://" + srchost));
		MongoClient tgtClient = new MongoClient(new MongoClientURI("mongodb://" + tgthost));
		MongoClient logClient = new MongoClient(new MongoClientURI("mongodb://" + loghost));

		_log = logClient.getDB("resharder").getCollection("log");
		_adminDB = srcClient.getDB("admin");

		_targetns = targetns;
		String[] params = _targetns.split("\\.");
		_tgt = tgtClient.getDB(params[0]).getCollection(params[1]);
		_tgt.drop();
		
		_oplog = logClient.getDB("resharder").getCollection("oplog_out");
		_log.drop();

		_reshard = reshard;
		_key = key;

		_ns = namespace;
		_readBatch = readBatch;
		_writeBatch = writeBatch;
		_secondary = secondary;
	}

	public static void processArgs(String[] args) throws UnknownHostException {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "--source":
				MongoClient srcClient = new MongoClient(args[++i]);
				_adminDB = srcClient.getDB("admin");
				break;

			case "--target":
				MongoClient tgtClient = new MongoClient(args[++i]);
				break;

			case "--log":
				MongoClient logClient = new MongoClient(args[++i]);
				_log = logClient.getDB("resharder").getCollection("log");
				_oplog = logClient.getDB("resharder").getCollection("oplog_out");
				break;

			case "--readSecondary":
				_secondary = true;
				break;

			case "--readBatch":
				_readBatch = Integer.parseInt(args[++i]);
				break;

			case "--writeBatch":
				_writeBatch = Integer.parseInt(args[++i]);
				break;

			case "--reshard":
				_reshard = true;
				break;

			case "--key":
				_key = args[++i];
				break;
			}
		}
	}

	public static DBCollection get_tgtCollection() {
		return _tgt;
	}

	public static DBCollection get_srcCollection() {
		return _src;
	}

	public static boolean is_readSecondary() {
		return _secondary;
	}

	public static String get_Namepace() {
		return _ns;
	}

	public static String get_TargetNamepace() {
		return _targetns;
	}

	public static int get_readBatch() {
		return _readBatch;
	}

	public static int get_writeBatch() {
		return _writeBatch;
	}

	public static String get_key() {
		return _key;
	}

	public static boolean is_reshard() {
		return _reshard;
	}

	public static DBCollection get_oplog() {
		return _oplog;
	}

	public static DBCollection get_log() {
		return _log;
	}

	public static DB get_adminDB() {
		return _adminDB;
	}
}
