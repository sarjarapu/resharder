package com.mongodb.resharder;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;

import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import com.mongodb.DBObject;

/**
 * @author rhoulihan
 * 
 */
public class Chunk implements Comparable<Chunk> {
	private Object _min, _max;
	private String _shardkey, _shard;
	private Chunk _left, _right;

	/**
	 * @param pMin
	 *            an Object representing the minimum value of the Chunk range or
	 *            null if this is the lowest range
	 * @param pMax
	 *            an Object representing the minimum value of the Chunk range or
	 *            null if this is the lowest range
	 * @param pShard
	 *            the name of the Shard this Chunk belongs to
	 */
	public Chunk(Object pMin, Object pMax, String pShard) {
		DBObject min = (DBObject) pMin, max = (DBObject) pMax;
		_shardkey = max.toMap().keySet().toArray()[0].toString();
		_shard = pShard;

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

	/**
	 * Test if a key is in the range of any Chunk in the tree for this Shard
	 * 
	 * @param pKey
	 *            the key to test
	 * @return true if key is in a Chunk range on the tree, false if not
	 * @throws Exception
	 */
	public boolean isOrphan(Object pKey) {
		if (Config.is_hashed()) {
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");

				ByteBuffer buffer = ByteBuffer.allocate(16);
				buffer.put(md.digest(pKey.toString().getBytes("UTF-8")));
				buffer.flip();

				pKey = new Long(buffer.getLong());
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (pKey instanceof String) {
			String key = pKey.toString(), min = null, max = null;
			if (_min == null && _max == null)
				return false;

			if (_min != null)
				min = _min.toString();

			if (_max == null)
				max = _max.toString();

			if (min != null && min.compareTo(key) > 0) {
				if (_left == null)
					return true;

				return _left.isOrphan(pKey);
			}

			if (_max == null && min.compareTo(key) <= 0)
				return false;

			if (max != null && max.compareTo(key) <= 0) {
				if (_right == null)
					return true;

				return _right.isOrphan(pKey);
			}

			if (_min == null && max.compareTo(key) >= 0)
				return false;

			if (min.compareTo(key) <= 0 && max.compareTo(key) > 0)
				return false;

		} else if (pKey instanceof Integer || pKey instanceof Long) {
			Long key = Long.parseLong(pKey.toString()), min = null, max = null;
			if (_min == null && _max == null)
				return false;

			if (_min != null)
				min = Long.parseLong(_min.toString());

			if (_max != null)
				max = Long.parseLong(_max.toString());

			if (min != null && min.compareTo(key) > 0) {
				if (_left == null)
					return true;

				return _left.isOrphan(pKey);
			}

			if (_max == null && min.compareTo(key) <= 0)
				return false;

			if (max != null && max.compareTo(key) <= 0) {
				if (_right == null)
					return true;

				return _right.isOrphan(pKey);
			}

			if (_min == null && max.compareTo(key) >= 0)
				return false;

			if (min.compareTo(key) <= 0 && max.compareTo(key) > 0)
				return false;
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

		if (_min == null || o._max == null)
			return -1;

		if (_max == null || o._min == null)
			return 1;

		if (_min instanceof String) {
			String min = _min.toString(), max = _max.toString(), oMin = o._min.toString(), oMax = o._max.toString();
			val = min.compareTo(oMax) >= 0 ? -1 : max.compareTo(oMin) <= 0 ? 1 : 0;
		} else if (_min instanceof Integer) {
			Integer min = (Integer) _min, max = (Integer) _max, oMin = (Integer) o._min, oMax = (Integer) o._max;
			val = min.compareTo(oMax) >= 0 ? -1 : max.compareTo(oMin) <= 0 ? 1 : 0;
		}

		else if (_min instanceof Long) {
			Long min = (Long) _min, max = (Long) _max, oMin = (Long) o._min, oMax = (Long) o._max;
			val = min.compareTo(oMax) >= 0 ? -1 : max.compareTo(oMin) <= 0 ? 1 : 0;
		}

		return val;
	}

	public String get_shardkey() {
		return _shardkey;
	}

	public String get_shard() {
		return _shard;
	}
}
