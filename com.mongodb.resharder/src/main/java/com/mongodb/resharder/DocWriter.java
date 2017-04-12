package com.mongodb.resharder;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DocWriter implements Runnable {
	private final static Queue<DBObject> _queue = new ConcurrentLinkedQueue<DBObject>();

	private static AtomicBoolean _running = new AtomicBoolean(false);
	private static AtomicInteger _readers = new AtomicInteger(0);

	private String _host;

	public static void readerStarted() {
		_readers.incrementAndGet();
	}

	public static void readerStopped() {
		_readers.decrementAndGet();
	}

	public static boolean get_running() {
		return _running.get();
	}

	public static void push(DBObject obj) {
		_queue.offer(obj);
	}

	public void run() {
		_running.set(true);

		try {
			Stack<DBObject> buffer = new Stack<DBObject>();

			_host = Config.get_tgtCollection().getDB().getMongo().getAddress().getHost() + ":"
					+ Config.get_tgtCollection().getDB().getMongo().getAddress().getPort();
			_host = Config.get_nodes().findOne(new BasicDBObject("host", _host)).get("name").toString();
			new Node(Config.get_nodes().findOne(new BasicDBObject("name", "resharder"))).addConnection(_host, "writer");

			MessageLog.push("connected to " + Config.get_tgtCollection().getDB().getMongo().getConnectPoint(), this
					.getClass().getSimpleName() + ".");

			while (_running.get()) {
				while (!_queue.isEmpty()) {
					// Grab docs of the queue until the buffer is full
					buffer.push(_queue.poll());

					if (buffer.size() == Config.get_writeBatch()) {
						// Write the docs to the clone Collection and clear the
						// buffer
						Config.docWrite(buffer);
						buffer.clear();
					}
				}

				try {
					// check to see if we are still copying
					if (_readers.get() == 0) {
						Config.docWrite(buffer);
						buffer.clear();

						if (_running.compareAndSet(true, false)) {
							// start replaying the oplog if not already started
							// by another DocWriter
							MessageLog.push("All readers finshed, commencing Oplog replay...", this.getClass()
									.getSimpleName());

							Launcher._tp.execute(new OpLogWriter());
						}
					} else {
						Thread.sleep(100);
					}
				} catch (InterruptedException ex) {
					Config.docWrite(buffer);
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
			//Config.get_tgtCollection().getDB().requestDone();
			new Node(Config.get_nodes().findOne(new BasicDBObject("name", "resharder"))).removeConnection(_host,
					"writer");

			shutdown();
		}
	}

	public static void shutdown() {
		_running.set(false);
		CollectionScanner.shutdown();
	}
}
