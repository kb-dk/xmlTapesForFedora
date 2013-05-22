package dk.statsbiblioteket.metadatarepository.xmltapes.interfaces;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 12:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class StoreLock {

    private Object lock;

    public synchronized void lock(Object that){

        while (lock != that){
            if (lock == null){
                lock = that;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {

                }
            }
        }
    }

    public synchronized void unlock(Object that){
        if (lock == that){
            lock = null;
        }
        notifyAll();
    }
}
