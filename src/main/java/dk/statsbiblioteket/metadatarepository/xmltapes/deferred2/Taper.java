package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.util.Files;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/20/13
 * Time: 11:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class Taper extends AbstractDeferringArchive {

    private static final Logger log = LoggerFactory.getLogger(Taper.class);


    private final Timer timer;
    private  long delay;

    private Cache cache;

    private boolean closed = false;
    private boolean timerStopped = true;
    private TimerTask task;

    public Taper(Archive tapeArchive, File tapingDir, long delay) {
        super();
        super.setDelegate(tapeArchive);
        super.setDeferredDir(tapingDir);
        timer = new Timer();
        setDelay(delay);
    }

    private void startTimer() {

        if (task != null){
            task.cancel();
        }
        task = new TimerTask() {
            @Override
            public synchronized void run() {
                timerStopped = false;
                log.debug("Taping thread started");
                if (closed) {
                    cancel();
                    timerStopped = true;
                    return;
                }
                try {
                    saveAll();
                } catch (Exception e) {
                    log.error("Failed to save objects",e);
                }
            }
        };
        timer.schedule(task, 0, delay);
    }

    private File getTapingFile(File cacheResultFile)  {
        File cacheResultDir = cache.getDeferredDir();
        String name = cacheResultFile.getAbsolutePath().replaceFirst(
                Pattern.quote(cacheResultDir.getAbsolutePath()),
                "");
        return new File(getDeferredDir(),name);
    }

    private void saveAll() throws IOException {
        if (cache == null){
            return;
        }
        List<File> cacheFiles = cache.getCacheFiles();
        for (File cacheFile : cacheFiles) {
            File tapingFile = getTapingFile(cacheFile);
            Files.move(cacheFile,tapingFile,true);
        }

        /*Save the already enqueued files*/
        tapeTheTapingFiles();



    }


    private void tapeTheTapingFiles() throws IOException {
        List<File> tapingFiles = getCacheFiles();
        for (File tapingFile : tapingFiles) {
            URI id = getIDfromFile(tapingFile);
            //HERE WE NEED TO RECOGNIZE THAT THE BLOB IS DEAD
            if (isDeleted(tapingFile)){
                getDelegate().remove(id);
                Files.delete(tapingFile);
            } else {
                OutputStream tapeOut = getDelegate().createNew(id, tapingFile.length());
                InputStream tapingIn = getInputStream(id);
                IOUtils.copyLarge(tapingIn, tapeOut);
                tapingIn.close();
                tapeOut.close();
                Files.delete(tapingFile);
            }

        }
    }


    public void setCache(Cache cache) {
        this.cache = cache;
    }

    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        //This one will not be called
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(URI id) throws IOException {
        //This one will not be called
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        Collection<URI> cachingLayerIDs = cache.getCacheIDs(filterPrefix);
        Collection<URI> tapingLayerIDs = getCacheIDs(filterPrefix);

        return new NonDuplicatingIterator(getDelegate().listIds(filterPrefix),cachingLayerIDs,tapingLayerIDs);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        while (!timerStopped){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        getDelegate().close();

    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
        startTimer();
    }
}


