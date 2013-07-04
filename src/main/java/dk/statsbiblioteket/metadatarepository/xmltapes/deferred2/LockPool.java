package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/3/13
 * Time: 11:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class LockPool {

    private Map<String,Thread> locks;

    public LockPool() {
        locks = new HashMap<String, Thread>();
    }

    public synchronized void acquireLock(String key){
        while ( ! acquireLockNonBlocking(key)){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }

    }

    private synchronized boolean acquireLockNonBlocking(String key){
        Thread lockedThread = locks.get(key);
        Thread currentThread = Thread.currentThread();
        if (lockedThread == null){
            //not locked before, locking it to this thread
            locks.put(key,currentThread);
            return true;
        }
        if (lockedThread.equals(Thread.currentThread())){
            //already locked to this thread
            return true;
        } else { //locked to another thread
            return false;
        }
    }

    public synchronized void releaseLock(String key){
        if (acquireLockNonBlocking(key)){
            locks.remove(key);
        }
    }

}
