# jedis-tools

A bunch of objects and helpers that use
[Jedis](https://github.com/xetorthio/jedis "Jedis") and allow accessing
Redis through common Java APIs.

## RedisSortedSet:
Implements the Java SortedSet API on top of a Redis sorted set.

## RedisMap:
Implements the Java Map API on top of a Redis hash.

## RedisBlockingQueue:
Implements the Java BlockingQueue API on top of a Redis list (simple blocking producer / consumer schemes).

## Usage examples:

    public static void RedisMapTest() throws InterruptedException
    {
        RedisMap map = new RedisMap()
        {
                protected final String m_strKey = "test:map";
                
                @Override
                protected final String getKey()
                {
                        return m_strKey;
                }
        };
        
        for (int i = 0; i < 10; i++)
        {
                map.put("key" + i, "value" + i);
        }

        // get start of day as unix time stamp
        Long today = System.currentTimeMillis()/1000;
        today -= (today % (3600*24));
        
        map.increment("page:homepage:views:" + today, 12L);
        map.increment("page:mailbox:views:" + today, 62L);
        map.increment("page:onlinenow:views:" + today, 15L);

        map.foreach(new Work<Boolean, Entry<String, String>> () 
        {
                @Override
                public Boolean work(Entry<String, String> e)
                {
                        System.out.println("key: " + e.getKey() + ", value: " + e.getValue());                
                        return true;
                }                        
        });
    }


    public static void RedisBlockingQueueTest() throws InterruptedException
    {
        RedisBlockingQueue q = new RedisBlockingQueue()
        {                                                
                protected final String m_strKey = "test:bq";
                
                @Override
                protected final String getKey()
                {
                        return m_strKey;
                }
        };
        
        // will wait
        System.out.println(q.take());
            
        for (int i = 0; i < 100; i++)
        {
                 q.add("" + i);
        }
        
        q.foreach(new Work<Boolean, String>() 
        { 
                 @Override
                 public Boolean work(String t)
                 {
                        System.out.println("Found: " + t);
                        return true;
                 }
        });
    }


    public static void RedisMailboxTest() throws Exception
    {                    
        // load up the mailbox from Redis and print it
        final Mailbox m = new Mailbox(1501571L);
        System.out.println("Inbox: Conversations { ");
        m.getInbox().foreach(new Work<Boolean, Tuple>()
        {
                public Boolean work(Tuple t)
                {                                                                                                
                        Conversation c = m.getInbox().getConversation(t.getElement());
                        
                        System.out.print("  Conversation: [id=" + t.getElement() + ", time=" + 
                                        new Date((long) t.getScore()) + "] [messages=");
                        c.foreach(new Work<Boolean, Tuple>() 
                        { 
                                public Boolean work(Tuple t)
                                {
                                        Calendar cal = Calendar.getInstance();
                                        cal.setTime(new Date((long) t.getScore()));
                                        String strDate = cal.get(Calendar.YEAR) + "/" + cal.get(Calendar.MONTH) + "/" + cal.get(Calendar.DATE);
                                        System.out.print("(" + t.getElement() + "," + strDate + ") ");
                                        return true; 
                                }
                        });
                        System.out.println("]");
                                                
                        return true;
                }
        });                
        System.out.println("}");                
    }


More examples are included in the source.

Copyright 2011 Hisham Mardam-Bey <hisham.mardambey@gmail.com>

