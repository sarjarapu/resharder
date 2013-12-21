package com.mongodb.resharder;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class Config {
	private static String _srcURI="mongodb://localhost:27017", _tgtURI="mongodb://localhost:28017", _logURI="mongodb://localhost:28017", _ns, _targetns;
	private static DBCollection _src, _tgt, _log;
	private static DB _adminDB;
	private static boolean _readSecondary = false;
	private static int _readBatch, _writeBatch;
	
	public static void processArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]){
			case "--source":
				_srcURI = args[++i];
				break;
				
			case "--target":
				_tgtURI = args[++i];
				break;
				
			case "--log":
				_logURI = args[++i];
				break;
				
			case "--secondary":
				Boolean.parseBoolean(args[++i]);
				break;
			}
		}
	}

	public static String get_srcURI() {
		return _srcURI;
	}

	public static void set_srcURI(String _srcURI) {
		Config._srcURI = _srcURI;
	}

	public static String get_tgtURI() {
		return _tgtURI;
	}

	public static void set_tgtURI(String _tgtURI) {
		Config._tgtURI = _tgtURI;
	}

	public static String getLogURI() {
		return _logURI;
	}

	public static void setLogURI(String logURI) {
		Config._logURI = logURI;
	}

	public static DBCollection get_tgtCollection() {
		return _tgt;
	}

	public static void set_tgtCollection(DBCollection _tgtCollection) {
		Config._tgt = _tgtCollection;
	}

	public static DBCollection get_srcCollection() {
		return _src;
	}

	public static void set_srcCollection(DBCollection _srcCollection) {
		Config._src = _srcCollection;
	}

	public static boolean is_readSecondary() {
		return _readSecondary;
	}

	public static String get_Namepace() {
		return _ns;
	}

	public static void set_Namespace(String _ns) {
		Config._ns = _ns;
	}

	public static String get_TargetNamepace() {
		return _targetns;
	}

	public static void set_TargetNamepace(String _targetns) {
		Config._targetns = _targetns;
	}

	public static int get_readBatch() {
		return _readBatch;
	}

	public static void set_readBatch(int _readBatch) {
		Config._readBatch = _readBatch;
	}

	public static int get_writeBatch() {
		return _writeBatch;
	}

	public static void set_writeBatch(int _writeBatch) {
		Config._writeBatch = _writeBatch;
	}

	public static DBCollection get_log() {
		return _log;
	}

	public static void set_log(DBCollection _log) {
		Config._log = _log;
	}

	public static DB get_adminDB() {
		return _adminDB;
	}

	public static void set_adminDB(DB _adminDB) {
		Config._adminDB = _adminDB;
	}
}
