package com.mongodb.resharder;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import freemarker.template.SimpleHash;

public class ShardMapper {
	private static List<Shard> _shards = new ArrayList<Shard>();

	public static List<Shard> getShards() {
		return _shards;
	}

	// Connect to mongos and pull the shard configuration
	public static SimpleHash getShardingStatus(SimpleHash hash) {
		_shards = new ArrayList<Shard>();
		List<String> _collections = new ArrayList<String>();
		BasicDBObject _shardconf = new BasicDBObject();
		MongoClient _mongo;

		try {
			_mongo = new MongoClient();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		DB configDB = _mongo.getDB("config");
		DB adminDB = _mongo.getDB("admin");
		DB resharderDB = _mongo.getDB("resharder");

		_shardconf.put("version", configDB.getCollection("version").findOne());

		CommandResult result = adminDB.command(new BasicDBObject("listShards", 1));

		hash.put("mongos", result.get("serverUsed"));
		hash.put("shardInfo", result.toString());

		@SuppressWarnings("unchecked")
		List<DBObject> shards = (List<DBObject>) result.get("shards");

		hash.put("numShards", shards.size());
		int numRepl = 0, numServers = 0, numShards = 0;

		for (DBObject shard : shards) {
			numShards++;
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
				// We are not in Kansas anymore...
				return null;
			}

			if (servers.indexOf("/") > 0) {
				numRepl++;
				shard.put("isReplSet", false);
				_shards.add(new Shard(servers.split("/")[0], addrs, true));
			} else {
				_shards.add(new Shard("SERVER_" + numShards, addrs, false));
			}

			numServers += hosts.length;

			shard.put("hosts", hosts);
			shard.removeField("host");
		}

		_shardconf.put("shards", shards);

		hash.put("numRepl", numRepl);
		hash.put("numServers", numServers);

		DBCollection dbs = configDB.getCollection("databases");

		DBCursor dbCursor = null, collectionCursor = null;
		try {
			dbCursor = dbs.find().sort(new BasicDBObject("name", 1));
			shards = new ArrayList<DBObject>();
			while (dbCursor.hasNext()) {
				DBObject curDB = dbCursor.next();
				BasicDBObject database = new BasicDBObject();
				database.putAll(curDB);

				if (curDB.get("partitioned").equals(true)) {
					DBCollection partCols = configDB.getCollection("collections");

					Pattern regex = Pattern.compile("^" + StringEscapeUtils.escapeJava(curDB.get("_id").toString())
							+ "\\.");

					collectionCursor = partCols.find(new BasicDBObject("_id", regex)).sort(new BasicDBObject("_id", 1));

					List<BasicDBObject> collections = new ArrayList<BasicDBObject>();
					while (collectionCursor.hasNext()) {
						DBObject curColl = collectionCursor.next();

						if (curColl.get("dropped").equals(false)) {
							BasicDBObject collection = new BasicDBObject();
							collection.put("collection", curColl.get("_id"));
							_collections.add(curColl.get("_id").toString());
							collection.put("shard key", curColl.get("key"));

							DBCollection chunkColl = configDB.getCollection("chunks");
							BasicDBList info = (BasicDBList) chunkColl.group(new BasicDBObject("shard", 1),
									new BasicDBObject("ns", curColl.get("_id")), new BasicDBObject("nChunks", 0),
									"function( doc , out ){ out.nChunks++; }");

							List<BasicDBObject> chunks = new ArrayList<BasicDBObject>();
							int totalChunks = 0;
							for (Object obj : info) {
								BasicDBObject shard = (BasicDBObject) obj;
								totalChunks += shard.getInt("nChunks");

								DBCursor chunkCursor = chunkColl.find(new BasicDBObject("ns", curColl.get("_id")),
										new BasicDBObject("shard", curDB.get("host")))
										.sort(new BasicDBObject("min", 1));

								List<DBObject> al1 = new ArrayList<DBObject>();
								while (chunkCursor.hasNext()) {
									al1.add(chunkCursor.next());
								}
								shard.put("chunks", al1);
								chunks.add(shard);
							}

							collection.put("shards", chunks);
							collection.put("nChunks", totalChunks);
							collections.add(collection);
						}
						database.put("collections", collections);
					}
				}
				shards.add(database);
			}

			_shardconf.put("databases", shards);
			hash.put("collections", _shardconf.toString());
			hash.put("collList", _collections);

			DBCollection config = resharderDB.getCollection("config");
			if (config != null)
				config.drop();
			
			resharderDB.getCollection("config").insert(_shardconf);
		} finally {
			if (dbCursor != null)
				dbCursor.close();
			if (collectionCursor != null)
				collectionCursor.close();
		}

		return hash;
	}
}
