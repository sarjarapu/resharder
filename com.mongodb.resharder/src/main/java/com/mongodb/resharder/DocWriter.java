package com.mongodb.resharder;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DocWriter implements Runnable {
	private final static Queue<DBObject> _queue = new ConcurrentLinkedQueue<DBObject>();

	private int _size;
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private DBCollection _collection;

	public DocWriter(DBCollection collection, int size) {
		this._collection = collection;
		this._size = size;
		_running.set(true);
	}

	public static boolean get_running() {
		return _running.get();
	}

	public static void push(DBObject obj) {
		_queue.offer(obj);
	}

	int _stopCount = 0;

	public void run() {
		try {
			Stack<DBObject> buffer = new Stack<DBObject>();

			// use the same socket for all writes
			// TODO - determine if we need multiple writers to the same collection object
			_collection.getDB().requestStart();
			_collection.getDB().requestEnsureConnection();
			MessageLog.push("Writer connected to " + _collection.getDB().getMongo().getConnectPoint(), this.getClass().getSimpleName() + ".");

			while (_running.get()) {
				while (!_queue.isEmpty()) {
					_stopCount = 0;
					// Grab docs of the queue until the buffer is full
					buffer.push(_queue.poll());

					if (buffer.size() == _size) {
						// Write the docs to the clone Collection and clear the
						// buffer
						// TODO - is there an exception that might need to be
						// handled here?
						writeDocs(buffer);
						buffer.clear();
					}
				}

				try {
					if (++_stopCount > 10) {
						MessageLog.push("Timeout waiting for docs. Shutting down...", this.getClass().getSimpleName());
						writeDocs(buffer);
						shutdown();
					}
					// Wait for more docs
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					writeDocs(buffer);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			MessageLog.push("ERROR: " + e.getMessage(), this.getClass().getSimpleName());
			MessageLog.push("Shutting down clone operation...", this.getClass().getSimpleName());
		} finally {
			// close our connection
			MessageLog.push("Writer disconnected from " + _collection.getDB().getMongo().getConnectPoint(), this.getClass().getSimpleName() + ".");
			_collection.getDB().requestDone();
			shutdown();
		}
	}

	private void writeDocs(Stack<DBObject> buffer) {
		if (buffer.size() > 0) {
			// TODO - should there be a stronger write concern on this op than
			// default?
			_collection.insert(buffer.toArray(new DBObject[0]));
		}
	}

	private void shutdown() {
		_running.set(false);
		CollectionScanner.shutdown();
	}
}
