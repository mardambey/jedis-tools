package jedistools.examples;

import jedistools.RedisSortedSet;
import redis.clients.jedis.Tuple;

public class Folder extends RedisSortedSet
{
	protected String m_strName;
	
	protected Long m_lProfileId;
	
	protected String m_strKey;
					
	public Folder(String strName, Long lProfileId)
	{
		m_strName = strName;
		m_lProfileId = lProfileId;
		m_strKey = m_lProfileId + ":" + m_strName;
	}
	
	public Conversation getConversation(String strId)		
	{
		return new Conversation(Long.valueOf(strId));			
	}
	
	public Folder addConversation(Conversation c, Double tstamp)
	{
		add(new Tuple(String.valueOf(c.getId()), tstamp));			
		return this;
	}
	
	@Override
	protected String getKey()
	{			
		return m_strKey;
	}
}
