package com.mongodb.resharder;

import java.util.Arrays;
import java.util.Iterator;

import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import com.mongodb.DBObject;

public class Chunk implements Comparable<Chunk> {
	private Object _min, _max;
	private String _shardkey, _shard;
	private Chunk _left, _right;

	public Chunk(Object pMin, Object pMax, String shard) {
		DBObject min = (DBObject) pMin, max = (DBObject) pMax;
		_shardkey = max.toMap().keySet().toArray()[0].toString();
		_shard =shard;

		@SuppressWarnings("rawtypes")
		Iterator it = max.toMap().values().iterator();
		_max = it.next();
		if (_max instanceof MaxKey) {
			_max = null;
		}

		it = min.toMap().values().iterator();
		_min = it.next();
		if (_min instanceof MinKey) {
			_min = null;
		}
	}

	public boolean isOrphan(Object pKey) {
		if (pKey instanceof String) {
			//TODO this needs some debugging
			if (_min.toString().compareTo(pKey.toString()) <= 0 && _max.toString().compareTo(pKey.toString()) >= 0)
				return false;

			if (_min.toString().compareTo(pKey.toString()) >= 0 && _left != null)
				return _left.isOrphan(pKey);

			if (_max.toString().compareTo(pKey.toString()) < 0 && _right != null)
				return _right.isOrphan(pKey);
		} 
		else if (pKey instanceof Integer) {
			Integer min = (Integer) _min, max = (Integer) _max, key = (Integer) pKey;

			if (_max == null && min.compareTo(key) <= 0)
					return false;
			else if (_min == null && max.compareTo(key) >= 0)
					return false;
			else if (min.compareTo(key) <= 0 && max.compareTo(key) >= 0)
				return false;

			if (min.compareTo(key) > 0 && _left != null)
				return _left.isOrphan(pKey);

			if (max.compareTo(key) < 0 && _right != null)
				return _right.isOrphan(pKey);
		} 
		else if (pKey instanceof Long) {
			Long min = (Long) _min, max = (Long) _max, key = (Long) pKey;

			if (_max == null) {
				if (min.compareTo(key) <= 0)
					return false;
			} else if (_min == null) {
				if (max.compareTo(key) >= 0)
					return false;
			} else if (min.compareTo(key) <= 0 && max.compareTo(key) >= 0)
				return false;
			
			if (min.compareTo(key) <= 0 && max.compareTo(key) >= 0)
				return false;

			if (min.compareTo(key) > 0 && _left != null)
				return _left.isOrphan(pKey);

			if (max.compareTo(key) < 0 && _right != null)
				return _right.isOrphan(pKey);
		}

		return true;
	}

	public Chunk load(Chunk[] chunks, int start, int end) {
		Arrays.sort(chunks);
		if (start > end) {
			return null;
		}
		int mid = (start + end) / 2;

		chunks[mid]._left = chunks[mid].load(chunks, start, mid - 1);
		chunks[mid]._right = chunks[mid].load(chunks, mid + 1, end);

		return chunks[mid];
	}

	@Override
	public int compareTo(Chunk o) {
		int val = 0;

		if (_min == null)
			return -1;

		if (_max == null)
			return 1;

		if (_min instanceof String)
			val = (_min.toString().compareTo(o._max.toString()) < 0) ? -1 : (_max.toString().compareTo(
					o._min.toString()) > 0) ? 1 : 0;

		else if (_min instanceof Integer)
			val = (Integer.parseInt(_min.toString()) < (int) o._max) ? -1 : (_max != null || Integer.parseInt(_max
					.toString()) < (int) o._min) ? 1 : 0;

		else if (_min instanceof Long)
			val = (_min != null || (long) _min < (long) o._max) ? -1
					: (_max != null && (long) _max < (long) o._min) ? 1 : 0;

		return val;
	}

	public Chunk get_right() {
		return _right;
	}

	public void set_right(Chunk _right) {
		this._right = _right;
	}

	public Chunk get_left() {
		return _left;
	}

	public void set_left(Chunk _left) {
		this._left = _left;
	}

	public String get_shardkey() {
		return _shardkey;
	}

	public String get_shard() {
		return _shard;
	}
}
