package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class Config {
	private static String _ns, _targetns, _reshardKey;
	private static DBCollection _src, _tgt, _log, _oplog, _nodes;
	private static DB _adminDB, _configDB, _logDB;
	private static boolean _secondary = false, _reshard = false, _isCLI = false, _done = false;
	private static int _readBatch, _writeBatch;
	private static Map<String, List<Chunk>> _chunks = new HashMap<String, List<Chunk>>();
	private static AtomicLong _docCount = new AtomicLong(0), _orphanCount = new AtomicLong(0),
			_oplogCount = new AtomicLong(0), _oplogReads = new AtomicLong(0);
	private static MongoClientURI _srcURI, _tgtURI, _logURI;
	private static Map<String, String> _props = new HashMap<String, String>();
	private static long _ts = 0, _lastDocPerSec, _lastOrphanPerSec;

	public static void init(Map<String, String> props) {

		// TODO - check if a clone is in process and prompt to kill it
		// for now kill any resharder threads that might be running
		if (OpLogWriter.isRunning()) {
			MessageLog.push("ERROR:, clone operation in progress.", Config.class.getSimpleName());
		}
		
		_done = false;
		_ts = 0;

		if (props != null) {
			for (Entry<String, String> prop : props.entrySet()) {
				setProperty(prop.getKey(), prop.getValue());
			}
		}

		loadDefaults();

		_chunks = new HashMap<String, List<Chunk>>();

		_docCount.set(0);
		_orphanCount.set(0);
		_oplogCount.set(0);
		_oplogReads.set(0);
		
		_nodes = _logDB.getCollection("nodes");
		
		if (props == null)
			_nodes.drop();
		
	}

	public static boolean validate() {
		if (_srcURI == null || _tgtURI == null || _logURI == null || _ns.isEmpty() || _targetns.isEmpty()
				|| (_reshard && _reshardKey.isEmpty())) {
			System.out.println("Incomplete Configuration\n");
			return false;
		}

		return true;
	}

	public static void print() {
		System.out.println();
		System.out.println("Current Configuration");
		System.out.println();
		System.out.println("source:        " + _props.get("source"));
		System.out.println("target:        " + _props.get("target"));
		System.out.println("log:           " + _props.get("log"));
		System.out.println("namespace:     " + _props.get("namespace"));
		System.out.println("targetns:      " + _props.get("targetns"));
		System.out.println("readSecondary: " + _props.get("readSecondary"));
		System.out.println("reshard:       " + _props.get("reshard"));
		System.out.println("key:           " + _props.get("key"));
		System.out.println("readBatch:     " + _props.get("readBatch"));
		System.out.println("writeBatch:    " + _props.get("writeBatch"));
		System.out.println();
	}

	public static Map<String, Long> getCounters() {
		Map<String, Long> map = new HashMap<String, Long>();

		map.put("docCount", new Long(_docCount.get()));
		map.put("orphanCount", new Long(_orphanCount.get()));
		map.put("oplogCount", new Long(_oplogCount.get()));
		map.put("oplogReads", new Long(_oplogReads.get()));

		if (_ts > 0) {
			long secs = ((System.currentTimeMillis() - _ts) / 1000);
			if (secs == 0) {
				secs = 1;
			}

			if (DocWriter.get_running()) {
				map.put("docsPerSec", map.get("docCount") / secs);
				map.put("orphansPerSec", map.get("orphanCount") / secs);
				_lastDocPerSec = map.get("docsPerSec");
				_lastOrphanPerSec = map.get("orphansPerSec");
			} else {
				map.put("docsPerSec", _lastDocPerSec);
				map.put("orphansPerSec", _lastOrphanPerSec);
			}

			map.put("oplogsPerSec", map.get("oplogCount") / secs);

		} else {
			map.put("docsPerSec", new Long(0));
			map.put("orphansPerSec", new Long(0));
			map.put("oplogsPerSec", new Long(0));
			map.put("oplogReads", new Long(0));

			if (map.get("docCount") > 0)
				_ts = System.currentTimeMillis();
		}

		return map;
	}

	public static void oplogCopy() {
		_oplogReads.incrementAndGet();
	}

	public static void oplogWrite(DBObject doc) throws Exception {
		if (Config.get_oplog() != null) {
			Config.get_oplog().insert(doc);
		} else {
			throw new Exception("Unable to write oplog data, no collection has been set for output.");
		}
		Config._oplogCount.incrementAndGet();
	}

	public static void docWrite(List<DBObject> docs) throws UnknownHostException {
		if (docs.size() > 0) {
			// for (DBObject doc : docs) {
			// _tgt.save(doc);
			// }

			if (_docCount.get() == 0) {
				String[] params = _targetns.split("\\.");
				_tgt = new MongoClient(_tgtURI).getDB(params[0]).getCollection(params[1]);
			}

			_tgt.insert(docs.toArray(new DBObject[0]));

			Config._docCount.addAndGet(docs.size());
		}
	}

	public static void processArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i]);
			if (!args[i].equals("--console")) {
				if (i + 1 < args.length) {
					setProperty(args[i].replaceAll("-", ""), args[++i]);
				}
			} else {
				_isCLI = true;
			}
		}

		loadDefaults();
	}

	private static void loadDefaults() {
		if (!_props.containsKey("srchost"))
			setProperty("srchost", "localhost:27017");

		if (!_props.containsKey("tgthost"))
			setProperty("tgthost", "localhost:27017");

		if (!_props.containsKey("loghost"))
			setProperty("loghost", "localhost:28017");

		if (!_props.containsKey("namespace"))
			setProperty("namespace", "test.grades");

		if (!_props.containsKey("targetns"))
			setProperty("targetns", "resharder.clone");

		if (!_props.containsKey("secondary"))
			setProperty("secondary", "true");

		if (!_props.containsKey("reshard"))
			setProperty("reshard", "true");

		if (!_props.containsKey("key"))
			setProperty("key", "{_id:\"hashed\"}");

		if (!_props.containsKey("readBatch"))
			setProperty("readBatch", "100");

		if (!_props.containsKey("writeBatch"))
			setProperty("writeBatch", "50");
	}

	public static String setProperty(String prop, String val) {
		String ret = "Bad Configuration Property:  " + prop;
		MongoClient mongo;

		try {
			switch (prop) {
			case "srchost":
				_srcURI = new MongoClientURI("mongodb://" + val);
				mongo = new MongoClient(_srcURI);

				_adminDB = mongo.getDB("admin");
				_configDB = mongo.getDB("config");

				ret = "Source host set to " + val;
				_props.put("source", val);
				break;

			case "tgthost":
				_tgtURI = new MongoClientURI("mongodb://" + val);
				mongo = new MongoClient(_tgtURI);

				String[] params = _targetns.split("\\.");
				_tgt = mongo.getDB(params[0]).getCollection(params[1]);

				ret = "Target host set to " + val;
				_props.put("target", val);
				break;

			case "loghost":
				_logURI = new MongoClientURI("mongodb://" + val);
				mongo = new MongoClient(_logURI);

				_logDB = mongo.getDB("resharder");
				_log = _logDB.getCollection("log");
				_oplog = _logDB.getCollection("oplog_out");
				_log.drop();
				_oplog.drop();
				_oplog.ensureIndex(new BasicDBObject("ts", 1));

				ret = "Log host set to " + val;
				_props.put("log", val);
				break;

			case "secondary":
				_secondary = Boolean.parseBoolean(val);
				ret = "Secondary read set to " + val;
				_props.put("readSecondary", val);
				break;

			case "readBatch":
				_readBatch = Integer.parseInt(val);
				ret = "Read batch size set to " + val;
				_props.put("readBatch", val);
				break;

			case "writeBatch":
				_writeBatch = Integer.parseInt(val);
				ret = "Write batch size set to " + val;
				_props.put("writeBatch", val);
				break;

			case "reshard":
				_reshard = Boolean.parseBoolean(val);
				ret = "Reshard on copy set to " + val;
				_props.put("reshard", val);
				break;

			case "key":
				_reshardKey = val;
				ret = "Reshard key set to " + val;
				_props.put("key", val);
				break;

			case "namespace":
				_ns = val;
				ret = "Source collection namespace set to " + val;
				_props.put("namespace", val);
				break;

			case "targetns":
				_targetns = val;
				ret = "Target collection namespace set to " + val;
				_props.put("targetns", val);
				break;
			}
		} catch (Exception e) {
			ret = "Bad value for " + prop + " ERROR: " + e.getMessage();
		}

		return ret;
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
		Config._orphanCount.incrementAndGet();
	}

	public static boolean isCLI() {
		return _isCLI;
	}

	public static DBCollection get_nodes() {
		return _nodes;
	}

	public static boolean isDone() {
		return _done;
	}

	public static void done(boolean done) {
		Config._done = done;
	}
}
