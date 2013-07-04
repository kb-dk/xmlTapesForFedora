package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.util.Files;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

/**
 * Cache is a fedora storage implementation that just holds files in a cache dir. Writes happen to the tempdir, and
 * when the stream is closed, the file is moved to the cache dir. Read operations are resolved against the cache dir,
 * and if not found delegated.
 */
public class Cache extends AbstractDeferringArchive {

    private static final Logger log = LoggerFactory.getLogger(Cache.class);


    public static final String TEMP_PREFIX = "temp";
    private final File tempDir;
    private TimerTask task;
    private Timer timer;

    private boolean timerStopped;
    private boolean closed;
    /**
     * The delay between runs of the taper thread
     */
    private long delay;

    /**
     * The maximum allowed age of a file before it will be taped
     */
    private long tapeDelay;


    public Cache(File cacheDir, File tempDir) throws IOException {
        super();
        super.setDeferredDir(cacheDir);
        this.tempDir = tempDir.getCanonicalFile();
        this.tempDir.mkdirs();
        timer = new Timer();

        delay = 1000;
        tapeDelay = 2000;
        setDelay(delay);


    }

    private File getTempFile(URI id) throws IOException {
        File tempfile = File.createTempFile(URLEncoder.encode(id.toString(), UTF_8), TEMP_PREFIX, tempDir);
        tempfile.deleteOnExit();
        return tempfile;
    }


    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        File tempFile = getTempFile(id);
        return new CacheOutputStream(tempFile, getDeferredFile(id), lockPool);

    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return new NonDuplicatingIterator(getDelegate().listIds(filterPrefix),getCacheIDs(filterPrefix));
    }

    @Override
    public synchronized void remove(URI id) throws IOException {
        File cacheFile = getDeferredFile(id);
        lockPool.acquireLock(cacheFile.getName());
        cacheFile.delete();
        cacheFile.createNewFile();
        setDeleted(cacheFile);
        lockPool.releaseLock(cacheFile.getName());

    }


    private void tapeTheTapingFile(File fileToTape) throws IOException {
        URI id = getIDfromFile(fileToTape);

        if (isDeleted(fileToTape)) {
            getDelegate().remove(id);
            Files.delete(fileToTape);
        } else {
            OutputStream tapeOut = getDelegate().createNew(id, fileToTape.length());
            InputStream tapingIn = getInputStream(id);
            IOUtils.copyLarge(tapingIn, tapeOut);
            tapingIn.close();
            tapeOut.close();
            Files.delete(fileToTape);
        }

    }


    private void startTimer() {

        if (task != null) {
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
                    log.error("Failed to save objects", e);
                }
            }
        };
        timer.schedule(task, 0, delay);
    }


    private void saveAll() throws IOException {

        List<File> cacheFiles = getAndLockCacheFiles();

        long now = System.currentTimeMillis();

        for (File cacheFile : cacheFiles) {
            try {
                if (cacheFile.lastModified() + tapeDelay < now) {
                    tapeTheTapingFile(cacheFile);
                } else {
                    continue;
                }
            } finally {
                lockPool.releaseLock(cacheFile.getName());
            }


        }

    }


    public void setDelay(long delay) {
        this.delay = delay;
        startTimer();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        while (!timerStopped) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        getDelegate().close();

    }

}
