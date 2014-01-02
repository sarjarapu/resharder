package com.mongodb.resharder;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OpLogWriter implements Runnable {
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private static AtomicInteger _processed = new AtomicInteger(0);

	public static boolean get_running() {
		return _running.get();
	}

	int _stopCount = 0;

	public void run() {
		_running.set(true);

		try {
			Stack<DBObject> buffer = new Stack<DBObject>();

			// use the same socket for all writes
			// TODO - determine if we need multiple writers to the same
			// collection object
			Config.get_oplog().getDB().requestStart();
			Config.get_oplog().getDB().requestEnsureConnection();

			MessageLog.push("connected to " + Config.get_oplog().getDB().getMongo().getConnectPoint(), this.getClass()
					.getSimpleName() + ".");

			DBCursor oplog = Config.get_oplog().find().sort(new BasicDBObject("ts", 1)).skip(_processed.get())
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

				while (!oplog.hasNext()) {
					try {
						oplog.close();
						oplog = Config.get_oplog().find().sort(new BasicDBObject("ts", 1)).skip(_processed.get())
								.limit(Config.get_readBatch());

						if (!oplog.hasNext()) {
							MessageLog.push("No OpLog entries found, sleeping for 10 seconds...", this.getClass()
									.getSimpleName());

							// Wait for more docs
							Thread.sleep(10000);
						}
					} catch (InterruptedException ex) {
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			MessageLog.push("ERROR: " + e.getMessage(), this.getClass().getSimpleName());
			MessageLog.push("Shutting down clone operation...", this.getClass().getSimpleName());
		} finally {
			// close our connection
			MessageLog.push("disconnected from " + Config.get_tgtCollection().getDB().getMongo().getConnectPoint(),
					this.getClass().getSimpleName() + ".");
			Config.get_oplog().getDB().requestDone();
			shutdown();
		}
	}

	public static void shutdown() {
		_running.set(false);
	}
}
