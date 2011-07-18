package jedistools;

import static jedistools.JedisFactory.withJedisDo;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import jedistools.JedisFactory.JWork;
import jedistools.JedisFactory.Work;

/**
 * Implements an input / output layer over a Redis hash 
 * through a Java {@link Map}. Can be used to persist 
 * storage objects that can leverage counters as field 
 * types with atomic incr/decr operations.
 * 
 * @author Hisham Mardam-Bey
 *
 */
public abstract class RedisMap extends RedisBaseObject implements Map<String, String>
{	
	@Override
	public int size()
	{
		return withJedisDo(new JWork<Integer>() 
		{
			@Override
			public Integer work(Jedis j)
			{
				return j.hlen(getFullKey()).intValue();				
			}			
		});
	}

	@Override
	public boolean isEmpty()
	{
		return size() == 0;
	}

	@Override
	public boolean containsKey(final Object key)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				return j.hexists(getFullKey(), key.toString());				
			}			
		});
	}

	/**
	 * This is an expensive operations because it 
	 * has to fetch all the values in the map.
	 */
	@Override
	public boolean containsValue(final Object value)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				return j.hkeys(getFullKey()).contains(value);				
			}			
		});
	}

	@Override
	public String get(final Object key)
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				return j.hget(getFullKey(), key.toString());				
			}			
		});
	}

	@Override
	public String put(final String key, final String value)
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				j.hset(getFullKey(), key, value);
				return value;
			}			
		});
	}

	/**
	 * Note that this implementation of remove does 
	 * not return the value of the key that was 
	 * removed because this will mean 2 operations 
	 * for Redis. It will return the suppied key 
	 * instead.
	 */
	@Override
	public String remove(final Object key)
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				j.hdel(getFullKey(), key.toString());
				return key.toString();
			}			
		});
	}

	@Override
	public void putAll(final Map<? extends String, ? extends String> m)
	{
		withJedisDo(new JWork<Object>() 
		{
			@SuppressWarnings("unchecked")
			@Override
			public Object work(Jedis j)
			{
				j.hmset(getFullKey(), (Map<String, String>) m);
				return null;
			}			
		});
	}

	@Override
	public void clear()
	{
		withJedisDo(new JWork<Object>() 
		{
			@Override
			public Object work(Jedis j)
			{
				j.del(getFullKey());
				return null;
			}			
		});
	}

	@Override
	public Set<String> keySet()
	{
		return withJedisDo(new JWork<Set<String>>() 
		{
			@Override
			public Set<String> work(Jedis j)
			{
				return j.hkeys(getFullKey());				
			}			
		});
	}

	@Override
	public Collection<String> values()
	{
		return withJedisDo(new JWork<Collection<String>>() 
		{
			@Override
			public Collection<String> work(Jedis j)
			{
				return j.hvals(getFullKey());				
			}			
		});
	}

	@Override
	public Set<java.util.Map.Entry<String, String>> entrySet()
	{
		return withJedisDo(new JWork<Set<java.util.Map.Entry<String, String>>>() 
		{
			@Override
			public Set<java.util.Map.Entry<String, String>> work(Jedis j)
			{
				return j.hgetAll(getFullKey()).entrySet();
			}			
		});
	}	
	
	/**
	 * Returns all the values for the given {@link Collection} of 
	 * keys. If the collection converts to an array and maintains 
	 * its sorting order then the returned collection will be sorted 
	 * in the same order.
	 *  
	 * @param c the {@link Collection} of keys to get
	 * @return the {@link Collection} of values
	 */
	public Collection<String> getAll(final Collection<? extends String> c)
	{
		return withJedisDo(new JWork<Collection<String>>() 
		{			
			@Override
			public Collection<String> work(Jedis j)
			{
				return j.hmget(getFullKey(), c.toArray(new String[]{}));				
			}			
		});
	}
	
	/**
	 * Iterates over all elements in the map and runs 
	 * the given callback on them. If the callback  
	 * returns <code>false</code> at any time the iteration 
	 * process will abort.
	 * 
	 * @param work the {@link Work} to perform in every value
	 */
	public void foreach(Work<Boolean, Entry<String, String>> work)
	{
		Iterator<Entry<String, String>> iter = entrySet().iterator();
		
		while(iter.hasNext())
		{			
			if (!work.work(iter.next())) return;			
		}		
	}
	
	public Long increment(final String strKey, final Long intBy)
	{
		return withJedisDo(new JWork<Long>() 
		{			
			@Override
			public Long work(Jedis j)
			{
				return j.hincrBy(getFullKey(), strKey, intBy);				
			}			
		});
	}
}
