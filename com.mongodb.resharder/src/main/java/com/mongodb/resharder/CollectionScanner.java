/**
 * 
 */
package com.mongodb.resharder;

import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class CollectionScanner implements Runnable {
	private DBCollection _source = null;
	private boolean _running = false;
	private static AtomicBoolean _shutdown = new AtomicBoolean(false);
	private int _numread = 0;
	private Chunk _chunkTree;
	private String _host;

	public CollectionScanner(DBCollection source, Chunk root) {
		this._source = source;
		this._chunkTree = root;
	}

	public void run() {
		_running = true;
		DocWriter.readerStarted();

		// use the same socket for all reads
		try {
			_shutdown.set(false);

			_source.getDB().requestStart();
			_source.getDB().requestEnsureConnection();

			_host = _source.getDB().getMongo().getAddress().getHost() + ":"
					+ _source.getDB().getMongo().getAddress().getPort();
			_host = Config.get_nodes().findOne(new BasicDBObject("host", _host)).get("name").toString();
			new Node(Config.get_nodes().findOne(new BasicDBObject("name", "resharder"))).addConnection(_host, "reader");

			MessageLog.push("connected to " + _source.getDB().getMongo().getConnectPoint() + ".", this.getClass()
					.getSimpleName());

			while (_running && !_shutdown.get()) {
				DBCursor cursor = _source.find().sort(new BasicDBObject("$natural", 1)).skip(_numread)
						.limit(Config.get_readBatch());

				if (!cursor.hasNext()) {
					MessageLog.push("Collection scan completed...", this.getClass().getSimpleName());

					_running = false;
				}
				try {

					while (cursor.hasNext()) {
						DBObject doc = cursor.next();

						// Check if orphan
						if (!_chunkTree.isOrphan(doc.get(_chunkTree.get_shardkey()))) {
							// Put docs on queue
							DocWriter.push(doc);
						} else {
							Config.orphanDropped();
						}
						_numread++;
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					// close the cursor and socket
					cursor.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			new Node(Config.get_nodes().findOne(new BasicDBObject("name", "resharder"))).removeConnection(_host,
					"reader");

			_source.getDB().requestDone();
			MessageLog.push("disconnected from " + _source.getDB().getMongo().getConnectPoint() + ".", this.getClass()
					.getSimpleName());

			DocWriter.readerStopped();
			_shutdown.set(false);
		}
	}

	public static void shutdown() {
		_shutdown.set(true);
	}
}
