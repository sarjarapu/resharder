package com.mongodb.resharder;

import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OpLogReader implements Runnable {
	private BSONTimestamp _ts;
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private DBCollection _oplog = null;

	public OpLogReader(DBCollection oplog) {
		this._oplog = oplog;
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
			while (_running.get()) {
				_oplog.getDB().requestStart();
				_oplog.getDB().requestEnsureConnection();
				MessageLog.push("Reader connected to " + _oplog.getDB().getMongo().getConnectPoint() + ".", this.getClass().getSimpleName());
				
				// TODO - What happens when multiple threads call this concurrently on the same client?
				Config.get_oplog().getDB().requestStart();
				Config.get_oplog().getDB().requestEnsureConnection();
				MessageLog.push("Reader outputting to " + Config.get_oplog().getDB().getMongo().getConnectPoint() + ".", this.getClass().getSimpleName());

				DBCursor cursor = _oplog.find(new BasicDBObject("ns","test.grades")).sort(new BasicDBObject("$natural", -1)).limit(1);
				if (cursor.hasNext()) {
					DBObject doc = cursor.next();
					_ts = (BSONTimestamp) doc.get("ts");
				} else {
					_ts = new BSONTimestamp();
				}
				

				BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", _ts)).append("ns", Config.get_Namepace());
				cursor = _oplog.find(query).sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE)
						.addOption(Bytes.QUERYOPTION_AWAITDATA);

				try {
					while (cursor.hasNext() && _running.get()) {
						if (Config.get_oplog() != null) {
							Config.get_oplog().insert(cursor.next());
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
					MessageLog.push("Reader disconnected from " + _oplog.getDB().getMongo().getConnectPoint() + ".", this.getClass().getSimpleName());
					_oplog.getDB().requestDone();
					Config.get_oplog().getDB().requestDone();
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
