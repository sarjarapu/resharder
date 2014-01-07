package com.mongodb.resharder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OpLogReader implements Runnable {
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private static List<OpLogReader> _readers = new ArrayList<OpLogReader>();

	private BSONTimestamp _ts;
	private DBCollection _oplog = null;
	private DBCursor _cursor;

	public OpLogReader(DBCollection oplog) {
		this._oplog = oplog;
		_running.set(true);
	}

	public void setTimestamp(BSONTimestamp ts) {
		this._ts = ts;
	}
	
	public void killCursor() {
		_cursor.close();
	}
	
	public static void shutdown() {
		_running.set(false);
		for (OpLogReader reader : _readers) {
			reader.killCursor();
		}
		
		_readers = new ArrayList<OpLogReader>();
	}

	public void run() {
		_running.set(true);
		_readers.add(this);

		while (_running.get()) {
			_oplog.getDB().requestStart();
			_oplog.getDB().requestEnsureConnection();
			MessageLog.push("connected to " + _oplog.getDB().getMongo().getConnectPoint() + ".", this.getClass()
					.getSimpleName());

			Config.get_oplog().getDB().requestStart();
			Config.get_oplog().getDB().requestEnsureConnection();
			MessageLog.push("outputting to " + Config.get_oplog().getDB().getMongo().getConnectPoint() + ".", this
					.getClass().getSimpleName());

			_cursor = _oplog.find(new BasicDBObject("ns", "test.grades"))
					.sort(new BasicDBObject("$natural", -1)).limit(1);

			if (_cursor.hasNext()) {
				DBObject doc = _cursor.next();
				_ts = (BSONTimestamp) doc.get("ts");
			} else {
				if (_ts == null)
					_ts = new BSONTimestamp();
			}

			BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", _ts)).append("ns",
					Config.get_Namepace());
			_cursor = _oplog.find(query).sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE)
					.addOption(Bytes.QUERYOPTION_AWAITDATA);

			try {
				while (_cursor.hasNext() && _running.get()) {
					Config.oplogWrite(_cursor.next());
				}
			} catch (Exception e) {
				if (e instanceof InterruptedException) {
					// NOOP
				} else
					e.printStackTrace();
			} finally {
				if (_cursor != null)
					_cursor.close();
				MessageLog.push("disconnected from " + _oplog.getDB().getMongo().getConnectPoint() + ".", this
						.getClass().getSimpleName());
				_oplog.getDB().requestDone();
				Config.get_oplog().getDB().requestDone();
			}
		}
	}
}
