package jedistools;


/**
 * Base class for Java objects that wrap Redis data 
 * types with a Java native API.
 * 
 * @author Hisham Mardam-Bey
 *
 */
public abstract class RedisBaseObject
{		
	/**
	 * Returns the key used when interacting with 
	 * the store keys in Redis. To be implemented 
	 * by the child.
	 * 
	 * @return a String representing the key
	 */
	protected abstract String getKey();

	protected String REDIS_STORE_VERSION = "0";
	
	protected String REDIS_STORE_PREFIX = "rs:" + REDIS_STORE_VERSION;
	
	protected String m_strFullKey;
	
	protected String getFullKey()
	{
		if (m_strFullKey == null)
		{
			m_strFullKey = REDIS_STORE_PREFIX + ":" + getKey();
		}
		
		return m_strFullKey; 
	}		
}
