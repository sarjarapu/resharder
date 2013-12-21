package com.mongodb.resharder;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class OpLogReader implements Runnable {
	private BSONTimestamp _ts;
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private DBCollection _oplog = null, _tgtOplog = null;
	private String _ns;

	public OpLogReader(DBCollection oplog, DBCollection tgtOplog, String ns) {
		this._ns = ns;
		this._oplog = oplog;
		this._tgtOplog = tgtOplog;
		_running.set(true);
	}

	public static boolean get_running() {
		return _running.get();
	}

	public static void set_running(boolean running) {
		_running.set(running);
	}

	public void run() {
		try {
			MessageLog.push("Starting Oplog Reader...", "Launcher");
			while (_running.get()) {
				_oplog.getDB().requestStart();
				_oplog.getDB().requestEnsureConnection();
				_tgtOplog.getDB().requestStart();
				_tgtOplog.getDB().requestEnsureConnection();

				DBCursor cursor = _oplog.find(new BasicDBObject("ns","test.grades")).sort(new BasicDBObject("$natural", -1)).limit(1);
				if (cursor.hasNext()) {
					DBObject doc = cursor.next();
					_ts = (BSONTimestamp) doc.get("ts");
				} else {
					_ts = new BSONTimestamp();
				}
				

				BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", _ts)).append("ns", _ns);
				cursor = _oplog.find(query).sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE)
						.addOption(Bytes.QUERYOPTION_AWAITDATA);

				MessageLog.push("OpLog query string: " + cursor.getQuery().toString(), this.getClass().getSimpleName());
				MessageLog.push("Initial OpLog records: " + cursor.count(), this.getClass().getSimpleName());

				try {
					while (cursor.hasNext() && _running.get()) {
						if (_tgtOplog != null) {
							_tgtOplog.insert(cursor.next());
						} else {
							throw new Exception("Unable to write oplog data, no collection has been set for output.");
						}
					}
				} finally {
					try {
						if (cursor != null)
							cursor.close();
					} catch (final Throwable t) { /* NOOP */
					}
					_oplog.getDB().requestDone();
					_tgtOplog.getDB().requestDone();
				}

				try {
					Thread.sleep(100);
				} catch (final InterruptedException ie) {
					break;
				}
			}
		} catch (final Throwable t) {
			t.printStackTrace();
		}
	}
}
