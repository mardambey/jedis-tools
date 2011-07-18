package jedistools;

import static jedistools.JedisFactory.withJedisDo;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import jedistools.JedisFactory.JWork;
import jedistools.JedisFactory.Work;

/**
 * Implements an input / output layer over a Redis list 
 * through a Java {@link BlockingQueue}. This is primarily 
 * built to allow for very simple producer / consumer flows 
 * over Redis.
 * 
 * @author Hisham Mardam-Bey
 *
 */
public abstract class RedisBlockingQueue extends RedisBaseObject implements BlockingQueue<String>
{	
	@Override
	public String poll()
	{
		try
		{
			return take();
		}
		catch(InterruptedException e)
		{
			return null;
		}
	}

	@Override
	public String element()
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				List<String> ret = j.lrange(getFullKey(), 0, 0);
				
				if (ret.size() != 1)
				{
					throw new NoSuchElementException("The Redis list is either empty or an error occured.");
				}
				
				return ret.get(0);
			}			
		});
	}

	@Override
	public String peek()
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				List<String> ret = j.lrange(getFullKey(), 0, 0);
				
				if (ret.size() != 1)
				{
					return null;
				}
				
				return ret.get(0);
			}			
		});
	}

	@Override
	public int size()
	{
		return withJedisDo(new JWork<Integer>() 
		{
			@Override
			public Integer work(Jedis j)
			{
				return j.llen(getFullKey()).intValue();				
			}			
		});
	}

	@Override
	public boolean isEmpty()
	{
		return size() == 0;
	}

	@Override
	public Iterator<String> iterator()
	{
		return withJedisDo(new JWork<Iterator<String>>() 
		{
			@Override
			public Iterator<String> work(Jedis j)
			{
				List<String> ret = j.lrange(getFullKey(), 0, -1);
				return ret.iterator();
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
				List<String> ret = j.lrange(getFullKey(), 0, -1);
				return ret.toArray();
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
	public boolean addAll(final Collection<? extends String> c)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				Transaction t = j.multi();
				for (String s : c)
				{
					t.lpush(getFullKey(), s);
				}
				
				t.exec();
				return true;
			}			
		});
	}
	
	@Override
	public void clear()
	{
		withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				j.del(getFullKey());
				return true;
			}			
		});		
	}

	@Override
	public boolean add(final String e)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				j.lpush(getFullKey(), e);
				return true;
			}			
		});
	}

	@Override
	public boolean offer(String e)
	{
		return add(e);
	}

	@Override
	public void put(String e) throws InterruptedException
	{
		add(e);
	}

	@Override
	public boolean offer(String e, long timeout, TimeUnit unit) throws InterruptedException
	{
		return add(e);
	}

	@Override
	public String take() throws InterruptedException
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				List<String> ret = j.brpop(0, getFullKey());
				
				if (ret.size() != 2)
				{
					return null;
				}
				
				return ret.get(1);
			}			
		});
	}

	@Override
	public String poll(final long timeout, final TimeUnit unit) throws InterruptedException
	{
		return withJedisDo(new JWork<String>() 
		{
			@Override
			public String work(Jedis j)
			{
				Long t = TimeUnit.SECONDS.convert(timeout, unit);
				List<String> ret = j.brpop(t.intValue(), getFullKey());
				
				if (ret.size() != 2)
				{
					return null;
				}
				
				return ret.get(1);
			}			
		});
	}

	@Override
	public int remainingCapacity()
	{
		return Integer.MAX_VALUE;	
	}

	@Override
	public boolean remove(final Object o)
	{
		return withJedisDo(new JWork<Boolean>() 
		{
			@Override
			public Boolean work(Jedis j)
			{
				// if we get back 1 then we know we removed the 
				// element we need to remove so we return true
				return (j.lrem(getFullKey(), 1, o.toString()) == 1);
			}			
		});
	}

	@Override
	public boolean contains(Object o)
	{
		throw new UnsupportedOperationException("Can not determine if a " + 
				this.getClass().getName() + " contains an element.");
	}

	@Override
	public int drainTo(final Collection<? super String> c)
	{
		return withJedisDo(new JWork<Integer>() 
		{
			@Override
			public Integer work(Jedis j)
			{
				Transaction t = j.multi();
				Response<List<String>> resp = t.lrange(getFullKey(), 0, -1);
				t.del(getFullKey());
				t.exec();
				
				List<String> ret = resp.get();
				
				if (ret == null)
				{
					return 0;
				}
				
				if (c == null)
				{
					throw new NullPointerException("Specified collection can not be null.");
				}
				
				c.addAll(ret);				
				return ret.size();				
			}			
		});
	}

	@Override
	public int drainTo(final Collection<? super String> c, final int maxElements)
	{
		return withJedisDo(new JWork<Integer>() 
		{
			@Override
			public Integer work(Jedis j)
			{
				Transaction t = j.multi();
				Response<List<String>> resp = t.lrange(getFullKey(), 0, maxElements - 1);
				t.ltrim(getFullKey(), 0, maxElements - 1);
				t.exec();
				
				List<String> ret = resp.get();
				
				if (ret == null)
				{
					return 0;
				}
				
				if (c == null)
				{
					throw new NullPointerException("Specified collection can not be null.");
				}
				
				c.addAll(ret);				
				return ret.size();				
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
	public void foreach(Work<Boolean, String> work)
	{
		Iterator<String> iter = iterator();
		
		while(iter.hasNext())
		{
			String t = iter.next();
			
			if (!work.work(t)) return;			
		}		
	}
	
	@Override
	public boolean removeAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean retainAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean containsAll(Collection<?> c)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String remove()
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}
}
