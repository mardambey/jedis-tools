package jedistools.examples;

import jedistools.RedisSortedSet;
import redis.clients.jedis.Tuple;

public class Conversation extends RedisSortedSet
{
	protected Long m_lConversationId;
	protected String m_strKey;
	
	public Conversation(Long lConversationId)
	{
		m_lConversationId = lConversationId;
		m_strKey = "conv:" + lConversationId;
	}
	
	public Long getId()
	{
		return m_lConversationId;
	}
	
	public Conversation addMsg(Long lMsgId, Double tstamp)
	{
		add(new Tuple(String.valueOf(lMsgId), tstamp));
		return this;
	}
	
	public Conversation addMsg(Tuple t)
	{						
		add(t);
		return this;
	}
	
	@Override
	protected String getKey()
	{
		return m_strKey;
	}	
}
