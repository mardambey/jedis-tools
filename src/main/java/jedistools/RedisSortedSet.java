package jedistools;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import jedistools.JedisFactory.JWork;

import static jedistools.JedisFactory.*;

/**
 * Implements an input / output layer over a Redis sorted set 
 * through a Java {@link SortedSet}.
 * 
 * @author Hisham Mardam-Bey
 *
 */
public abstract class RedisSortedSet extends RedisBaseObject implements SortedSet<Tuple>
{		
	@Override
	public int size()
	{
		return withJedisDo(new JWork<Integer>() 
		{
			@Override
			public Integer work(Jedis j)
			{
				return j.zcard(getFullKey()).intValue();
			}			
		});
	}

	@Override
	public boolean isEmpty()
	{		
		return size() == 0;
	}

	/**
	 * Checks whether the provided {@link String} exists in the set.
	 * 
	 * @param key the string to check for
	 * @return true if the string exists, false otherwise
	 */
	@Override
	public boolean contains(final Object key)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				String strKey = (String) (key);
				
				if (strKey == null) return false;
				
				return j.zscore(getFullKey(), strKey) != null;				
			}			
		});
	}

	@Override
	public Iterator<Tuple> iterator()
	{
		return withJedisDo(new JWork<Iterator<Tuple>>() 
		{
			@Override
			public Iterator<Tuple> work(Jedis j)
			{
				Set<Tuple> res = j.zrangeByScoreWithScores(getFullKey(), Double.MIN_VALUE, Double.MAX_VALUE);
				
				if (res == null) return null;
				
				return res.iterator();
			}			
		});
	}

	@Override
	public Object[] toArray()
	{
		return withJedisDo(new JWork<Object[]>() 
		{
			@Override
			public Object[] work(Jedis j)
			{
				Set<Tuple> res = j.zrangeByScoreWithScores(getFullKey(), Double.MIN_VALUE, Double.MAX_VALUE);
				
				if (res == null) return null;
				
				return res.toArray();
			}			
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a)
	{
		return (T[]) toArray();
	}
	
	@Override
	public boolean add(final Tuple e)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				j.zadd(getFullKey(), e.getScore(), e.getElement());
				return true;
			}			
		});
	}

	/**
	 * Removes the provided {@link String} from the set.
	 * 
	 * @param o the string to remove
	 * @return true all the time (TODO: fix this)
	 */
	@Override	
	public boolean remove(final Object o)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				if (!(o instanceof String)) return false;
				
				j.zrem(getFullKey(), (String)o);
				return true;
			}			
		});
	}

	/**
	 * Checks whether the provided {@link String}s exists in the set.
	 * 
	 * @return true if the strings exists, false otherwise
	 */
	@Override
	public boolean containsAll(final Collection<?> c)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{				
				Set<String> res = j.zrangeByScore(getFullKey(), MINUS_INF, PLUS_INF);
				return res.containsAll(c);
			}			
		});
	}

	@Override
	public boolean addAll(final Collection<? extends Tuple> c)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{				
				Transaction t = j.multi();
						
				for (Object o : c)
				{
					Tuple tuple = (Tuple) o;
					t.zadd(getFullKey(), tuple.getScore(), tuple.getElement());
				}

				t.exec();
				return true;
			}			
		});
	}

	/**
	 * Clears the set and retains the given {@link Tuple} {@link Collection}.
	 * 
	 * @return true all the time (TODO: fix this)
	 */
	@Override
	public boolean retainAll(final Collection<?> c)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{				
				@SuppressWarnings("unused")
				List<Object> ret = j.multi(new TransactionBlock()
				{					
					@Override
					public void execute() throws JedisException
					{
						del(getFullKey());
						
						for (Object o : c)
						{
							Tuple t = (Tuple) o;				
							zadd(getFullKey(), t.getScore(), t.getElement());
						}
					}
				});
				
				return true;
			}			
		});
	}

	/**
	 * Removes the given {@link Collection} of {@link String}s from the set.
	 * 
	 * @return true all the time (TODO: fix this)
	 */
	@Override
	public boolean removeAll(final Collection<?> c)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{				
				@SuppressWarnings("unused")
				List<Object> ret = j.multi(new TransactionBlock()
				{					
					@Override
					public void execute() throws JedisException
					{
						for (Object o : c)
						{
							String s = (String) o;					
							zrem(getFullKey(), s);
						}
					}
				});
				
				return true;
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
	public Comparator<? super Tuple> comparator()
	{
		// we use Tuple's natural ordering so we're allowed to return null
		return null;
	}

	@Override
	public SortedSet<Tuple> subSet(final Tuple fromElement, final Tuple toElement)
	{
		return withJedisDo(new JWork<SortedSet<Tuple>>() 
		{
			@Override
			public SortedSet<Tuple> work(Jedis j)
			{								
				Set<Tuple> res = j.zrangeByScoreWithScores(getFullKey(), fromElement.getScore(), toElement.getScore());
				// SortedSet.subSet() forbids the passed in toElement
				// from being in the set
				res.remove(toElement);
				
				TreeSet<Tuple> ret = new TreeSet<Tuple>();
				ret.addAll(res);
				
				return ret;
			}			
		});
	}

	@Override
	public SortedSet<Tuple> headSet(final Tuple toElement)
	{
		return withJedisDo(new JWork<SortedSet<Tuple>>() 
		{
			@Override
			public SortedSet<Tuple> work(Jedis j)
			{								
				Set<Tuple> res = j.zrangeByScoreWithScores(getFullKey(), Double.MIN_VALUE, toElement.getScore());
				// SortedSet.headSet() forbids the passed in element
				// from being in the set
				res.remove(toElement);
				
				TreeSet<Tuple> ret = new TreeSet<Tuple>();
				ret.addAll(res);
				
				return ret;
			}			
		});
	}

	@Override
	public SortedSet<Tuple> tailSet(final Tuple fromElement)
	{
		return withJedisDo(new JWork<SortedSet<Tuple>>() 
		{
			@Override
			public SortedSet<Tuple> work(Jedis j)
			{								
				Set<Tuple> res = j.zrangeByScoreWithScores(getFullKey(), fromElement.getScore(), Double.MAX_VALUE);
				TreeSet<Tuple> ret = new TreeSet<Tuple>();
				ret.addAll(res);
				
				return ret;
			}			
		});
	}

	@Override
	public Tuple first()
	{
		return withJedisDo(new JWork<Tuple>() 
		{
			@Override
			public Tuple work(Jedis j)
			{								
				Set<Tuple> ret = j.zrangeWithScores(getFullKey(), 0, 0);
				
				if (ret == null || ret.size() != 1)
				{
					return null;
				}
				
				return ret.iterator().next();
			}			
		});
	}

	@Override
	public Tuple last()
	{
		return withJedisDo(new JWork<Tuple>() 
		{
			@Override
			public Tuple work(Jedis j)
			{								
				Set<Tuple> ret = j.zrangeWithScores(getFullKey(), -1, -1);
				
				if (ret == null || ret.size() != 1)
				{
					return null;
				}
				
				return ret.iterator().next();
			}			
		});
	}
	
	/**
	 * Iterates over all elements ({@link Tuple} objects) in the 
	 * store in order of score in the main Redis sorted set and 
	 * runs the supplied {@link Work} on each element. If work 
	 * return <code>false</code> at any time the iteration 
	 * process will abort.
	 * 
	 * @param work the {@link Work} to perform in every value
	 */
	public void foreach(Work<Boolean, Tuple> work)
	{
		Iterator<Tuple> iter = iterator();
		
		while(iter.hasNext())
		{
			Tuple t = iter.next();
			
			if (!work.work(t)) return;			
		}		
	}
}
