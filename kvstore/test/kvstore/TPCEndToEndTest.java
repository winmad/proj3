package kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {
	
    @Test(timeout = 15000)
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    }
	
    @Test(timeout = 15000)
    public void testMultiplePutGet() throws KVException {
    	client.put("foo" , "bar");
    	assertEquals("get failed" , client.get("foo") , "bar");
    	client.put("foo" , "barbar");
    	assertEquals(client.get("foo") , "barbar");
    	
    	try {
    		client.get("okay");
    		fail("NO_SUCH_KEY Exception not thrown!");
    	} 
    	catch (KVException ex) {
    		assertEquals(ex.getKVMessage().getMsgType() , KVConstants.RESP);
    		assertEquals(ex.getKVMessage().getMessage() , KVConstants.ERROR_NO_SUCH_KEY);
    	}
	        
    	try {
    		client.put(null , "xxx");
    		fail("should not reach here!");
    	}
    	catch (KVException ex) {
    		assertEquals(ex.getKVMessage().getMsgType() , KVConstants.RESP);
    		assertEquals(ex.getKVMessage().getMessage() , KVConstants.ERROR_INVALID_KEY);
    	}
	        
    	client.del("foo");
    	try {
    		client.get("foo");
    		fail("should not reach here!");
    	} 
    	catch (KVException e) {
    		assertEquals(e.getKVMessage().getMsgType() , KVConstants.RESP);
    		assertEquals(e.getKVMessage().getMessage() , KVConstants.ERROR_NO_SUCH_KEY);
    	}
    }
    
    @Test(timeout = 60000)
    public void testSingleSlaveCrash() throws KVException {
        client.put(KEY1 , "1");
        assertEquals(client.get(KEY1) , "1");
        
        try {
			stopSlave(new Long(SLAVE1).toString());
		} 
        catch (InterruptedException ex) {
			ex.printStackTrace();
		}

        // still work for GET if one replica fails
        try {
        	assertEquals(client.get(KEY1) , "1");
        }
        catch (KVException ex) {
        	fail("failed when get KEY1");
        }

        Thread thread1 = new Thread(
        		new Runnable() {
					public void run() {
						try {
							Thread.sleep(10000);
							try {
								startSlave(SLAVE1);
							} 
							catch (Exception ex) {
								ex.printStackTrace();
							}

							// recover from log
							try {
								assertEquals(client.get(KEY1) , "1");
							} 
							catch (KVException ex) {
								fail("failed when get KEY1");
							}

							try {
								client.put(KEY1 , "23");
							} 
							catch (KVException ex) {
								fail("failed when put (KEY1 , 23)");
							}

							try {
								assertEquals(client.get(KEY1) , "23");
							} 
							catch (KVException ex) {
								fail("failed when get KEY1");
							}
						} 
						catch (InterruptedException ex) {
						}
					}
				});
        
        Thread thread2 = new Thread(
				new Runnable() {
					public void run() {
						try {
							Thread.sleep(100);
						} 
						catch (InterruptedException ex) {
						}
						
						try {
							client.put(KEY1 , "2");
							fail("should not reach here: one replica failed");
						} 
						catch (KVException ex) {
							System.out.println(ex.getKVMessage().getMessage());
						}
						
						try {
							Thread.sleep(20000);
						}
						catch (InterruptedException ex) {							
						}
						
						try {
							assertEquals(client.get(KEY1) , "23");
						} 
						catch (KVException ex) {
							fail("failed when get KEY1");
						}
					}
				});
        
        thread1.start();
        thread2.start();
        
        try {
			Thread.sleep(300);
		} 
        catch (InterruptedException ex) {
		}
        
        while(thread1.isAlive() || thread2.isAlive()) {
        	try {
				Thread.sleep(1000);
			} 
        	catch (InterruptedException ex) {
			}
        }
    }
    
}
