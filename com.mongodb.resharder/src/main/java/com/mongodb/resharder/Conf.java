package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class Conf {
	private static String _ns, _targetns, _reshardKey;
	private static DBCollection _src, _tgt, _log, _oplog;
	private static DB _adminDB, _configDB, _logDB;
	private static boolean _secondary = false, _reshard = false;
	private static int _readBatch, _writeBatch;
	private static boolean _initialized = false;
	private static Map<String, List<Chunk>> _chunks = new HashMap<String, List<Chunk>>();
	private static AtomicLong _docCount = new AtomicLong(0), _orphanCount = new AtomicLong(0), _oplogCount = new AtomicLong(0);
	private static MongoClientURI _srcURI, _tgtURI, _logURI;

	public Conf(String namespace, String targetns, int readBatch, int writeBatch, boolean reshard, String key,
			boolean secondary, String srchost, String tgthost, String loghost) throws Exception {
		
		// TODO - check if a clone is in process and prompt to kill it
		// for now kill any resharder threads that might be running
		if (DocWriter.get_running()) {
			MessageLog.push("ERROR:, clone operation in progress.", this.getClass().getSimpleName());
			throw new Exception("ERROR:, clone operation in progress.");
		}

		_targetns = targetns;

		_reshard = reshard;
		_reshardKey = key;

		_ns = namespace;
		_readBatch = readBatch;
		_writeBatch = writeBatch;
		_secondary = secondary;
		
		_srcURI = new MongoClientURI("mongodb://" + srchost);
		_tgtURI = new MongoClientURI("mongodb://" + tgthost);
		_logURI = new MongoClientURI("mongodb://" + loghost);
		
		init();
	}
	
	public static void init() throws UnknownHostException {
		MongoClient srcClient = new MongoClient(_srcURI);
		MongoClient tgtClient = new MongoClient(_tgtURI);
		MongoClient logClient = new MongoClient(_logURI);

		_adminDB = srcClient.getDB("admin");
		_configDB = srcClient.getDB("config");
		_logDB = logClient.getDB("resharder");
		
		String[] params = _targetns.split("\\.");
		_tgt = tgtClient.getDB(params[0]).getCollection(params[1]);
		_tgt.drop();

		_log = _logDB.getCollection("log");
		_oplog = _logDB.getCollection("oplog_out");
		_log.drop();
		_oplog.drop();
		
		_chunks = new HashMap<String, List<Chunk>>();
		
		_docCount.set(0);
		_orphanCount.set(0);
		_oplogCount.set(0);
		
		_initialized = true;	
	}
	
	public static Map<String, Long> getCounters() {
		Map<String, Long> map = new HashMap<String, Long>();
		
		map.put("docCount", new Long(_docCount.get()));
		map.put("orphanCount", new Long(_orphanCount.get()));
		map.put("oplogCount", new Long(_oplogCount.get()));
		
		return map;
	}

	public static void oplogWrite(DBObject doc) throws Exception {
		if (Conf.get_oplog() != null) {
			Conf.get_oplog().insert(doc);
		} else {
			throw new Exception("Unable to write oplog data, no collection has been set for output.");
		}
		Conf._oplogCount.incrementAndGet();
	}

	public static void docWrite(List<DBObject> docs) throws UnknownHostException {
		if (docs.size() > 0) {
//			for (DBObject doc : docs) {
//				_tgt.save(doc);
//			}
			
			if (_docCount.get() == 0) {
				String[] params = _targetns.split("\\.");
				_tgt = new MongoClient(_tgtURI).getDB(params[0]).getCollection(params[1]);
			}
			
			_tgt.insert(docs.toArray(new DBObject[0]));
			
			Conf._docCount.addAndGet(docs.size());
		}
	}

	public static void processArgs(String[] args) throws UnknownHostException {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "--source":
				MongoClient srcClient = new MongoClient(args[++i]);
				_adminDB = srcClient.getDB("admin");
				break;

			case "--target":
				@SuppressWarnings("unused")
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
				_reshardKey = args[++i];
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

	public static String get_reshardKey() {
		return _reshardKey;
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

	public static boolean isInitialized() {
		return _initialized;
	}

	public static DB get_configDB() {
		return _configDB;
	}

	public static DB get_logDB() {
		return _logDB;
	}

	public static Map<String, List<Chunk>> get_chunks() {
		return _chunks;
	}

	public static long get_orphans() {
		return _orphanCount.get();
	}

	public static void orphanDropped() {
		Conf._orphanCount.incrementAndGet();
	}
}
