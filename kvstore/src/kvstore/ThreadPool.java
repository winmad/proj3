package kvstore;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;


public class ThreadPool {

    /* Array of threads in the threadpool */
    private Thread threads[];
    LinkedList<Runnable> jobs;
    ReentrantLock addLock , getLock;

    /**
     * Constructs a Threadpool with a certain number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) {
        threads = new Thread[size];
        jobs = new LinkedList<Runnable>();
        addLock = new ReentrantLock();
        getLock = new ReentrantLock();
        
        for (int i = 0; i < size; i++)
        	threads[i] = new WorkerThread(this);
        for (int i = 0; i < size; i++)
        	threads[i].start();
    }

    /**
     * Add a job to the queue of jobs that have to be executed. As soon as a
     * thread is available, the thread will retrieve a job from this queue if
     * if one exists and start processing it.
     *
     * @param r job that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public void addJob(Runnable r) throws InterruptedException {
    	addLock.lock();
    	jobs.add(r);
    	addLock.unlock();
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     * @return A runnable task that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    private Runnable getJob() throws InterruptedException {
    	getLock.lock();
    	Runnable res = null;
        if (!jobs.isEmpty())
        	res = jobs.remove();
        getLock.unlock();
        return res;
    }

    /**
     * A thread in the thread pool.
     */
    private class WorkerThread extends Thread {

        private ThreadPool threadPool;

        /**
         * Constructs a thread for this particular ThreadPool.
         *
         * @param pool the ThreadPool containing this thread
         */
        public WorkerThread(ThreadPool pool) {
            threadPool = pool;
        }

        /**
         * Scan for and execute tasks.
         */
        @Override
        public void run() {
        	for (;;) {
	            try {
	            	Runnable job = threadPool.getJob();
	            	if (job != null)
	            		job.run();
	            }
	            catch (Exception ex) {
	            	return;
	            }
        	}
        }
    }
}
