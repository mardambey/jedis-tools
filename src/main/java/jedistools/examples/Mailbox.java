package jedistools.examples;

public class Mailbox
{
	protected Folder m_inbox;
	protected Folder m_sentbox;
	protected Long m_lProfileId;
	
	public Mailbox(Long lProfileId)
	{
		m_lProfileId = lProfileId;
		m_inbox = new Folder("inbox", m_lProfileId);
		m_sentbox = new Folder("sentbox", m_lProfileId);
	}
	
	public Folder getInbox()
	{
		return m_inbox;
	}
	
	public Folder getSentbox()
	{
		return m_sentbox;
	}
}


