package dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore;

import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.NonDuplicatingIterator;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.util.FileAlreadyExistsException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Timer;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class TapingStore extends AbstractDeferringArchive<TapeArchive> implements AkubraCompatibleArchive {


    private static final Logger log = LoggerFactory.getLogger(TapingStore.class);
    protected CacheStore cache;

    private Taper task;
    private Timer timer;


    /**
     * The delay between runs of the taper thread
     */
    private long delay;



    public TapingStore(File tapingDir) {
        super();
        super.setStoreDir(tapingDir);
        timer = new Timer("Timer:tapingStore",true);
    }


    @Override
    public void init() throws IOException {
        super.init();
        startTaper();
    }


    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        throw new UnsupportedOperationException();
    }

    private void startTaper() {
        log.debug("Taping timer started, will run every {} milliseconds", delay);
        timer.schedule(task, 0, delay);
    }

    private void stopTimer() {
        //Close the timer, THEN close the delegates. The other order would cause problems.
        if ( task.cancel() && !task.isTimerHaveRunAtLeastOnce()) {
            task.run();
        }
    }


    @Override
    public void remove(URI id) throws IOException {
        testClosed();
        //This is only called by cache.remove, and that method already locks the cache for writing

        while (true) {
            lockPool.lockForWriting();
            cache.lockPool.lockForWriting();
            try {
                log.debug("Removing {}", id);

                File tapingFile = getDeferredFileDeleted(id);
                if (tapingFile.exists()) {
                    log.debug(
                            "{} is already scheduled for removal. Wait sleep for now and return later",
                            tapingFile.getName());
                    //sleep and try again
                    throw new FileAlreadyExistsException(tapingFile.toString());
                }

                //Situations
                //1. File is in cache
                //2. File is not in cache, but in tapes
                //3. File is not in cache or tapes
                File cacheFile = cache.getDeferredFile(id);
                try { //Get file from cache

                    log.debug("File {} is in cache, so copy the current version to {}", id, tapingFile);

                    //write this content to the tapingFile
                    FileUtils.moveFile(cacheFile, tapingFile);
                    //It is now in taping, so remove it from the cache
                    FileUtils.deleteQuietly(cacheFile);

                } catch (FileNotFoundException e) { //file was not in cache
                    if (getDelegate().exist(id)) {//but in tapes
                        log.debug("File {} was not in cache, but is in tapes. Just mark it for deletion.", id);
                        FileUtils.touch(tapingFile);
                    } else { //nowhere, so ignore this delete
                        log.debug("File {} was nowhere, so ignore this remove", id);
                    }
                }
                break;


            } catch (FileAlreadyExistsException e) {
                //ignore this, just go to sleep and try again

            } finally {
                cache.lockPool.unlockForWriting();
                lockPool.unlockForWriting();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }

        }
    }


    public void setDelay(long delay) {
        this.delay = delay;
    }



    @Override
    public void close() throws IOException {

        stopTimer();
        super.close();
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) throws IOException {
        log.debug("Calling listIDs with arguments {}",filterPrefix);
        return new NonDuplicatingIterator(getDelegate().listIds(filterPrefix),
                cache.getCacheIDs(filterPrefix),getCacheIDs(filterPrefix));
    }



    public void setCache(CacheStore cache) {
        this.cache = cache;
    }

    public void setTask(Taper task) {
        this.task = task;
    }
}
