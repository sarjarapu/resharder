package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;

import freemarker.template.SimpleHash;

public class ShardMapper {
	private static List<Shard> _shards = new ArrayList<Shard>();

	public static List<Shard> getShards() {
		return _shards;
	}

	// TODO this class is very ugly but no time to cleanup this code, apologies
	// if it hurts your eyes to read

	// Connect to mongos and pull the shard configuration
	public static SimpleHash getShardingStatus(SimpleHash hash) {

		_shards = new ArrayList<Shard>();
		Config.init(null);

		List<String> _collections = new ArrayList<String>();

		// object to hold the current shard config
		BasicDBObject _shardconf = new BasicDBObject();

		_shardconf.put("version", Config.get_configDB().getCollection("version").findOne());

		CommandResult result = Config.get_adminDB().command(new BasicDBObject("listShards", 1));

		@SuppressWarnings("unchecked")
		List<DBObject> shards = (List<DBObject>) result.get("shards");

		if (hash != null) {
			hash.put("mongos", result.get("serverUsed"));
			hash.put("shardInfo", result.toString());
			hash.put("numShards", shards.size());
		}

		for (DBObject shard : shards) {
			String servers = shard.get("host").toString();

			String[] hosts = servers.split(",");

			if (hosts[0].contains("/"))
				hosts[0] = hosts[0].split("/")[1];

			List<ServerAddress> addrs = new ArrayList<ServerAddress>();

			try {
				for (String host : hosts) {
					String[] addr = host.split(":");
					addrs.add(new ServerAddress(addr[0], Integer.parseInt(addr[1])));
				}
			} catch (UnknownHostException e) {
				return null;
			}

			if (servers.indexOf("/") > 0) {
				shard.put("isReplSet", true);
				_shards.add(new Shard(servers.split("/")[0], addrs, true));
			} else {
				_shards.add(new Shard("", addrs, false));
			}

			shard.put("hosts", hosts);
			shard.removeField("host");
		}

		_shardconf.put("shards", shards);

		DBCursor dbCursor = null, collectionCursor = null;
		try {
			dbCursor = Config.get_configDB().getCollection("databases").find().sort(new BasicDBObject("name", 1));
			shards = new ArrayList<DBObject>();
			while (dbCursor.hasNext()) {
				DBObject curDB = dbCursor.next();
				BasicDBObject database = new BasicDBObject();
				database.putAll(curDB);

				if (curDB.get("partitioned").equals(true)) {
					Pattern regex = Pattern.compile("^" + StringEscapeUtils.escapeJava(curDB.get("_id").toString())
							+ "\\.");

					collectionCursor = Config.get_configDB().getCollection("collections")
							.find(new BasicDBObject("_id", regex)).sort(new BasicDBObject("_id", 1));

					List<BasicDBObject> collectionsList = new ArrayList<BasicDBObject>();
					while (collectionCursor.hasNext()) {
						DBObject curColl = collectionCursor.next();

						if (curColl.get("dropped").equals(false)) {
							BasicDBObject collection = new BasicDBObject();
							collection.put("collection", curColl.get("_id"));
							_collections.add(curColl.get("_id").toString());
							collection.put("shard key", curColl.get("key"));

							BasicDBList info = (BasicDBList) Config
									.get_configDB()
									.getCollection("chunks")
									.group(new BasicDBObject("shard", 1), new BasicDBObject("ns", curColl.get("_id")),
											new BasicDBObject("nChunks", 0), "function( doc , out ){ out.nChunks++; }");

							List<BasicDBObject> chunks = new ArrayList<BasicDBObject>();
							int totalChunks = 0;
							for (Object obj : info) {
								BasicDBObject chunk = (BasicDBObject) obj;
								totalChunks += chunk.getInt("nChunks");

								// Add list of chunks for this shard to the
								// config map
								String key = curColl.get("_id") + "." + chunk.getString("shard");
								List<Chunk> chunkList = new ArrayList<Chunk>();
								Config.get_chunks().put(key, chunkList);

								DBCursor chunkCursor = Config
										.get_configDB()
										.getCollection("chunks")
										.find(new BasicDBObject("ns", curColl.get("_id")).append("shard",
												chunk.getString("shard")),
												new BasicDBObject("shard", curDB.get("host")))
										.sort(new BasicDBObject("min", 1));

								List<DBObject> chunkDocs = new ArrayList<DBObject>();
								while (chunkCursor.hasNext()) {
									DBObject doc = (DBObject) chunkCursor.next();
									chunkDocs.add(doc);

									chunkList.add(new Chunk(doc.get("min"), doc.get("max"), chunk.getString("shard")));
								}
								chunk.put("chunks", chunkDocs);
								chunks.add(chunk);
							}

							collection.put("shards", chunks);
							collection.put("nChunks", totalChunks);
							collectionsList.add(collection);
						}
						database.put("collections", collectionsList);
					}
				}
				shards.add(database);
			}

			_shardconf.put("databases", shards);

			if (hash != null) {
				hash.put("collections", _shardconf.toString());
				hash.put("collList", _collections);
			}

			if (Config.get_logDB().getCollection("config") != null)
				Config.get_logDB().getCollection("config").drop();

			Config.get_logDB().getCollection("config").insert(_shardconf);
		} finally {
			if (dbCursor != null)
				dbCursor.close();
			if (collectionCursor != null)
				collectionCursor.close();
		}

		return hash;
	}
}
