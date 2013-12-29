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

	public static boolean isRunning() {
		return _running.get();
	}

	public static void shutdown() {

		// TODO this is not going to work if cursor is blocked waiting for a doc
		_running.set(false);
	}

	public void run() {
		try {
			_running.set(true);

			while (_running.get()) {
				_oplog.getDB().requestStart();
				_oplog.getDB().requestEnsureConnection();
				MessageLog.push("connected to " + _oplog.getDB().getMongo().getConnectPoint() + ".", this.getClass()
						.getSimpleName());

				// TODO - What happens when multiple threads call this
				// concurrently on the same client?
				Conf.get_oplog().getDB().requestStart();
				Conf.get_oplog().getDB().requestEnsureConnection();
				MessageLog.push("outputting to " + Conf.get_oplog().getDB().getMongo().getConnectPoint() + ".", this
						.getClass().getSimpleName());

				DBCursor cursor = _oplog.find(new BasicDBObject("ns", "test.grades"))
						.sort(new BasicDBObject("$natural", -1)).limit(1);

				if (cursor.hasNext()) {
					DBObject doc = cursor.next();
					_ts = (BSONTimestamp) doc.get("ts");
				} else {
					_ts = new BSONTimestamp();
				}

				BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", _ts)).append("ns",
						Conf.get_Namepace());
				cursor = _oplog.find(query).sort(new BasicDBObject("$natural", 1))
						.addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);

				try {
					while (cursor.hasNext() && _running.get()) {
						Conf.oplogWrite(cursor.next());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				finally {
					if (cursor != null)
						cursor.close();
					MessageLog.push("disconnected from " + _oplog.getDB().getMongo().getConnectPoint() + ".", this
							.getClass().getSimpleName());
					_oplog.getDB().requestDone();
					Conf.get_oplog().getDB().requestDone();
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
