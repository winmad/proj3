package kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {
	
	@Test(timeout = 15000)
	public void testFirstReplica() throws KVException {
		assertEquals(master.findFirstReplicaIndex(4611686018427387903L) , 0);
		assertEquals(master.findFirstReplicaIndex(4611686018427387904L) , 1);
		assertEquals(master.findFirstReplicaIndex(-4611686018427387902L) , 3);
	}
	
	
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
    public void testConcurrentRequest() throws KVException {
    	client.put(KEY3 , "2333");
    	assertEquals(client.get(KEY3) , "2333");
    	
    	Thread thread1 = new Thread(
    			new Runnable() {
    				public void run() {
    					try {
    						client.put(KEY3 , "2333333");
    					}
    					catch (KVException ex) {
    						fail("failed when update KEY3");
    					}
    					
    					try {
    						Thread.sleep(5000);
    					}
    					catch (InterruptedException ex) {    						
    					}
    					
    					try {
    						assertEquals(client.get(KEY3) , "x");
    					}
    					catch (KVException ex) {
    						fail("failed when get KEY3");
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
    						assertEquals(client.get(KEY3) , "2333333");
    					}
    					catch (KVException ex) {
    						fail("failed when get KEY3");
    					}
    					
    					try {
    						client.put(KEY3 , "x");
    					}
    					catch (KVException ex) {
    						fail("failed when update KEY3");
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
