package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:39 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class LockPool {

    public abstract void lockForWriting();

    public abstract void unlockForWriting();


}
