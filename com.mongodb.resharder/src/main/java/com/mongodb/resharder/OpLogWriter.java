package com.mongodb.resharder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OpLogWriter implements Runnable {
	private static AtomicBoolean _running = new AtomicBoolean(false), _active = new AtomicBoolean(true);
	private static AtomicInteger _processed = new AtomicInteger(0);

	public static boolean isRunning() {
		return _running.get();
	}

	public static boolean isActive() {
		return _active.get();
	}
	
	public static void shutdown() {
		_running.set(false);
	}

	public void run() {
		_running.set(true);
		_active.set(true);

		DBCursor oplog = null;
		try {
			// use the same socket for all writes
			Config.get_oplog().getDB().requestStart();
			Config.get_oplog().getDB().requestEnsureConnection();

			MessageLog.push("connected to " + Config.get_oplog().getDB().getMongo().getConnectPoint(), this.getClass()
					.getSimpleName() + ".");

			oplog = Config.get_oplog().find().sort(new BasicDBObject("ts", 1)).skip(_processed.get())
					.limit(Config.get_readBatch());

			while (_running.get()) {

				while (oplog.hasNext()) {
					DBObject doc = oplog.next();

					if (doc.get("op").toString().equals("i") || doc.get("op").equals("u")) {
						doc = (DBObject) doc.get("o");

						// update the record
						Config.get_tgtCollection().save(doc);
					} else if (doc.get("op").equals("d")) {
						doc = (DBObject) doc.get("o");

						// remove the record
						Config.get_tgtCollection().remove(doc);
					}

					Config.oplogCopy();
					_processed.incrementAndGet();
				}

				while (!oplog.hasNext() && _running.get()) {
					oplog.close();
					oplog = Config.get_oplog().find().sort(new BasicDBObject("ts", 1)).skip(_processed.get())
							.limit(Config.get_readBatch());

					if (!oplog.hasNext()) {
						_active.set(false);

						MessageLog.push("No pending OpLog entries found, sleeping for 10 seconds...", this.getClass()
								.getSimpleName());

						// Wait for more docs
						Thread.sleep(10000);
					}
				}
			}
		} catch (Exception e) {
			if (e instanceof InterruptedException) {
				// NOOP
			} else {
				e.printStackTrace();
				MessageLog.push("ERROR: " + e.getMessage(), this.getClass().getSimpleName());
			}
		} finally {
			// close our connection
			oplog.close();
			Config.get_oplog().getDB().requestDone();
			
			MessageLog.push("disconnected from " + Config.get_tgtCollection().getDB().getMongo().getConnectPoint(),
					this.getClass().getSimpleName() + ".");
			
			_running.set(false);
		}
	}
}
