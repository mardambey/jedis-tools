package jedistools;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A simple factory that returns a {@link Jedis} object back 
 * to the caller of {@link JedisFactory#getInstance()} after 
 * which they can call {@link JedisFactory#getRes()} to get 
 * a {@link Jedis} object and {@link JedisFactory#returnRes(Jedis)} to 
 * return the resource back to the pool.
 * 
 * @author Hisham Mardam-Bey
 *
 */
public class JedisFactory 
{
	protected static Logger m_logger = Logger.getLogger(JedisFactory.class.getName());
	
	/**
	 * Static for Redis' minus infinity.
	 */
	public static String MINUS_INF = "-inf";
	
	/**
	 * Static for Redis'  plus infinity.
	 */
	public static String PLUS_INF = "+inf";
	
	/**
	 * Host to connect to.
	 */
	protected static String REDIS_HOST = "localhost";
	
	/**
	 * Port to connect to.
	 */
	protected static int REDIS_PORT = 6379;
	
	/**
	 * Maximum number of allowed Jedis active connections in the pool.
	 */
	protected static int REDIS_POOL_MAX_ACTIVE = 32;
	
	/**
	 * Min number of allowed idle Jedis connections in the pool.
	 */
	protected static int REDIS_POOL_MIN_IDLE= 24;
	
	/**
	 * Max number of allowed idle Jedis connections in the pool.
	 */
	protected static int REDIS_POOL_MAX_IDLE = REDIS_POOL_MAX_ACTIVE;
	
	/**
	 * Number of times to try to get resources before giving up and reconnecting the entire pool.
	 */
	protected static int REDIS_FAILED_RESOURCES_BEFORE_RECONNECT = REDIS_POOL_MAX_ACTIVE / 2 + 1;
	
	/**
	 * Number of times to try to reconnect when needed.
	 */
	protected static int REDIS_RECONNECT_RETRY_COUNT = 48;
	
	/**
	 * Number of milliseconds to wait between every reconnect attempt.
	 */
	protected static int REDIS_RECONNECT_RETRY_WAITTIME = 5000;
	
	/**
	 * The connection pool.
	 */
	protected static JedisPool m_jedisPool;
	  	
	/**
	 * Need the following object to synchronize
	 * a block
	 */
	private static Object objSync = new Object();

	/**
	 *  Prevent direct access to the constructor 
	 */
	private JedisFactory() 
	{
		super();		
	}

	/**
	 * Gives the user access to a Redis object that they can use to interact with 
	 * the server. The {@link Jedis} object must be returned back using {@link JedisFactory#returnRes(Jedis)} so others can use it..
	 * 
	 * @return the Jedis object use, must be returned back using {@link JedisFactory#returnRes(Jedis)} so others can use it. 
	 */
	public static Jedis getRes() 
	{
		return maybeInitAndGet();
	}
	
	protected static void createAndConnectPool()
	{		
		if (m_jedisPool == null) 
		{						
			GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();					
			poolConfig.minIdle = REDIS_POOL_MIN_IDLE;
			poolConfig.maxIdle = REDIS_POOL_MIN_IDLE;
			poolConfig.maxActive = REDIS_POOL_MAX_ACTIVE;
			poolConfig.testOnBorrow = true;
						
			m_jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
		}    	
	}
		
	public static Jedis maybeInitAndGet()
	{
	    // in a non-thread-safe version of a singleton
	    // the following line could be executed, and the 
	    // thread could be immediately swapped out				
	    if (m_jedisPool == null) 
	    {
	    	synchronized(objSync) 
	    	{
	    		createAndConnectPool();
	    	}
	    }
	    
	    // get a working resource or null otherwise
	    Jedis j = getWorkingResource();
	    
	    if (j != null)
	    {	    
	    	return j;
	    }
	    
	    // at this point we could not find any resources to hand back or half our pool 
	    // is not connected, re-establish the connections
    	synchronized(objSync) 
    	{	  
    		m_logger.info("Could not find a working resource or more than 50% of the pool is dead, recreating pool and restablishing connections.");
    		
    		// TODO: when this happens everyone using the pool will get an NPE until we're back
    		m_jedisPool = null;
    		
    		for (int i = 0; i < REDIS_RECONNECT_RETRY_COUNT; i++)
    		{	    		
    			shutdownPool();
	    		createAndConnectPool();
	    		
	    		if (m_jedisPool != null)
	    		{
	    			Jedis jd = getWorkingResource();
	    			
	    			if (jd != null)
	    			{
	    				return jd;
	    			}	    				    			
	    		}
	    		
	    		// wait before we trying again, except for the last attempt
	    		if (i < REDIS_RECONNECT_RETRY_COUNT - 1)
	    		{
	    			try { Thread.sleep(REDIS_RECONNECT_RETRY_WAITTIME); } catch (Exception e) {}
	    		}
	    	}
	    }
    			    
	    return null;
	}
	  
	protected static void shutdownPool()
	{
		if (m_jedisPool == null)
		{			
			return;			
		}
		
		m_jedisPool.destroy();
		m_jedisPool = null;
	}
	
	/**
	 * Returns you a working resource or null if none are found.
	 * 
	 * @return the working {@link Jedis} resource.
	 */
	protected static Jedis getWorkingResource()
	{
		// try to find a working resource
	    for (int i = 0; i < REDIS_FAILED_RESOURCES_BEFORE_RECONNECT; i++)
	    {
	    	Jedis j = m_jedisPool.getResource();
	    
	    	if (j.isConnected())				
	    	{
	    		return j;
	    	}
	    	else 
	    	{
	    		m_jedisPool.returnBrokenResource(j);	    		
	    	}
	    }
	    
	    return null;
	}
	
	/**
	 * Returns the given {@link Jedis} object back to the connection pool so it can  be reused.
	 * 
	 * @param res the object to return
	 */
    public static void returnRes(Jedis res)
    {
    	if (m_jedisPool != null)
    	{
    		m_jedisPool.returnResource(res);
    	}
    }
    
    public static <T>  T withJedisDo(JWork<T> work)
    {
    	// catch exception and gracefully fall back.    	
    	try 
    	{
    		Jedis j = getRes();
    		T ret = work.work(j);
    		returnRes(j);
    	
    		return ret;
    	}
    	catch (Exception e)
    	{
    		return null;
    	}
    }        
    
    public interface Work<Return, Param>
    {
    	public Return work(Param p);
    }
    
    public interface JWork<Return> extends Work<Return, Jedis>
    {
    	
    }
}
