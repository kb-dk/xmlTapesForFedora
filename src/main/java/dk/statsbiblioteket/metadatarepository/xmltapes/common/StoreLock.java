package dk.statsbiblioteket.metadatarepository.xmltapes.common;

/**
 * This class attempts to ensure that only one process is capable of writing to the archive at a time.
 */
public class StoreLock {

    private Object lock;

    /**
     * Locks the storage to "me", meaning that only calls with "me" as the parameter will return from the method.
     * When called, the thread will go to "wait()" if it cannot aquire the lock. It will return when the lock have
     * been acquired
     * @param me the object to lock the storage to
     */
    public synchronized void lock(Object me){

        while (lock != me){
            if (lock == null){
                lock = me;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {

                }
            }
        }
    }

    /**
     * Unlock the store, if the store have been locked to "me". Notifies all the threads waiting to acquire the lock
     * @param me the object the storage was locked to
     */
    public synchronized void unlock(Object me){
        if (lock == me){
            lock = null;
        }
        notifyAll();
    }
}
