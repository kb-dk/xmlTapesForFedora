package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class LockPool {

    private ReentrantLock lock;


    public LockPool() {
        lock = new ReentrantLock();
    }

    public void lockForWriting() {
        lock.lock();
    }

    public void unlockForWriting() {
        lock.unlock();
    }

}
