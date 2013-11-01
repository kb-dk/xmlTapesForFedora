package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class LockPoolImpl
        extends LockPool {

    private ReentrantLock lock;


    public LockPoolImpl() {
        lock = new ReentrantLock();
    }

    @Override
    public void lockForWriting() {
        lock.lock();
    }

    @Override
    public void unlockForWriting() {
        lock.unlock();
    }

}
