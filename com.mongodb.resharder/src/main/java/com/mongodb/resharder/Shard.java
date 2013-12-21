package com.mongodb.resharder;

import java.util.List;

import com.mongodb.ServerAddress;

public class Shard {
	private String _name;
	private List<ServerAddress> _hosts;
	private boolean _isreplset;
	
	public Shard(String name, List<ServerAddress> hosts, boolean isreplset) {
		this._name = name;
		this._hosts = hosts;
		this._isreplset = isreplset;
	}
	
	public String getName() {
		return _name;
	}
	
	public List<ServerAddress> hosts() {
		return _hosts;
	}
	
	public boolean isReplSet() {
		return _isreplset;
	}
}
