package dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore;

import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.NonDuplicatingIterator;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeUtils;
import dk.statsbiblioteket.util.FileAlreadyExistsException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

    public static Collection<URI> getCacheIDs(String filterPrefix, List<File> cacheFiles) throws IOException {
        //Get the cached files
        ArrayList<URI> result = new ArrayList<URI>();
        for (File cacheFile : cacheFiles) {
            URI id = TapeUtils.getIDfromFileWithDeleted(cacheFile);
            if (id == null){
                continue;
            }

            if (filterPrefix != null && id.toString().startsWith(filterPrefix)){
                result.add(id);
            }
        }
        return result;
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

        while (true) {
            lockPool.lockForWriting();
            cache.lockPool.lockForWriting();
            testClosed();
            try {
                log.debug("Removing {}", id);

                File tapingFile = TapeUtils.getStoredFileDeleted(getStoreDir(),id);
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
                File cacheFile = TapeUtils.getStoredFile(cache.getStoreDir(),id);
                try { //Get file from cache

                    //write this content to the tapingFile
                    FileUtils.moveFile(cacheFile, tapingFile);

                    //It is now in taping, so remove it from the cache
                    FileUtils.deleteQuietly(cacheFile);
                    log.debug("File {} is in cache, so copy the current version to {}", cacheFile, tapingFile);

                } catch (FileNotFoundException e) { //file was not in cache
                    if (getDelegate().exist(id)) {//but in tapes
                        log.debug("File {} was not in cache, but is in tapes. Just mark it for deletion.", cacheFile);
                        FileUtils.touch(tapingFile);
                    } else { //nowhere, so ignore this delete
                        log.debug("File {} was nowhere, so ignore this remove", cacheFile);
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
        lockPool.lockForWriting();
        try {
            stopTimer();
            super.close();
        } finally {
            lockPool.unlockForWriting();
        }
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) throws IOException {
        log.debug("Calling listIDs with arguments {}",filterPrefix);
        lockPool.lockForWriting();
        try {
            return new NonDuplicatingIterator(getDelegate().listIds(filterPrefix),
                    getCacheIDs(filterPrefix, cache.getStoreFiles()), getCacheIDs(filterPrefix,this.getStoreFiles()));
        } finally {
            lockPool.unlockForWriting();
        }
    }



    public void setCache(CacheStore cache) {
        this.cache = cache;
    }

    public void setTask(Taper task) {
        this.task = task;
    }
}
