package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.LockPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class LockPoolImpl extends LockPool {

    private ReentrantLock lock;


    public LockPoolImpl() {
        lock = new ReentrantLock();
    }

    @Override
    public void lockForWriting() {
        while (true){
            try {
                if (lock.tryLock(10, TimeUnit.MILLISECONDS)){
                    break;
                }
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    @Override
    public void unlockForWriting() {
        lock.unlock();
    }

}
