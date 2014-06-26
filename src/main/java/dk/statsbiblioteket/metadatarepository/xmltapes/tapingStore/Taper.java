package dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore;

import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.LockPool;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.TimerTask;

public class Taper extends TimerTask {
    private final Logger log = LoggerFactory.getLogger(Taper.class);
    private final LockPool tapingLock;
    private final LockPool cacheLock;

    private final TapingStore tapingStore;
    private final CacheStore cacheStore;
    private final TapeArchive tapeArchive;



    private boolean timerHaveRunAtLeastOnce = false;

    /**
     * The maximum allowed age of a file before it will be taped
     */
    private long tapeDelay;


    public Taper(TapingStore tapingStore,CacheStore cacheStore, TapeArchive tapeArchive) {
        this.tapingStore = tapingStore;
        this.cacheStore = cacheStore;
        this.tapeArchive = tapeArchive;
        tapingLock = tapingStore.lockPool;
        cacheLock = cacheStore.lockPool;
    }

    @Override
    public synchronized void run() {
        try {
           saveAll();
        } catch (Exception e) {
            log.error("Failed to save objects", e);
        } finally {
            timerHaveRunAtLeastOnce = true;
        }
    }



    /**
     * Get all the files in cache, lock them all, and tape all that are above a certain age.
     *
     * @throws java.io.IOException
     */
    private void saveAll() throws IOException {
        //1. Tape all the files in the tapingDir (getStoreFiles)
        //2. lock cache for writes
        //3. Move all acceptable files from cache to taping dir
        //4. Tape all the files in the tapingDir (getStoreFiles)
        tapingLock.lockForWriting();
        try {
            tapingStore.testClosed();
            final File tapingStoreDir = tapingStore.getStoreDir();

            //log.debug("Attempting to save all");
            //1
            tapeAll(tapingStore.getStoreFiles());
            //2
            cacheLock.lockForWriting();
            try {
                //3
                long now = System.currentTimeMillis();
                List<File> cacheFiles = cacheStore.getStoreFiles();
                for (File cacheFile : cacheFiles) {
                    log.debug("Found file {} in caching folder", cacheFile.getName());
                    if (cacheFile.lastModified() + tapeDelay < now) {
                        log.debug("File {} is old enough, move to {}", cacheFile.getName(), tapingStoreDir);
                        FileUtils.moveFileToDirectory(cacheFile, tapingStoreDir, true);
                    } else {
                        log.debug("File {} is to young, ignore", cacheFile);
                        continue;
                    }
                }
            } finally {
                cacheLock.unlockForWriting();
            }
            tapeAll(tapingStore.getStoreFiles());
        } finally {
            tapingLock.unlockForWriting();
        }
    }

    private void tapeAll(List<File> tapingFiles) throws IOException {
        tapingLock.lockForWriting();
        tapingStore.testClosed();
        try {
            for (File tapingFile : tapingFiles) {
                if (TapeUtils.isDeleteFile(tapingFile)) {
                    tapeTheTapingFileDeletion(tapingFile);
                } else {
                    tapeTheTapingFileAddition(tapingFile);
                }
            }
        } finally {
            tapingLock.unlockForWriting();
        }
    }

    protected void tapeTheTapingFileAddition(File fileToTape) throws IOException {
        log.debug("Taping addition of file {}", fileToTape.getName());
        tapingLock.lockForWriting();
        try {
            URI id = TapeUtils.getIdfromFile(fileToTape);
            tapeArchive.tapeFile(id, fileToTape);
            FileUtils.deleteQuietly(fileToTape);
        } finally {
            tapingLock.unlockForWriting();
        }
    }


    protected synchronized void tapeTheTapingFileDeletion(File fileToTape) throws IOException {
        log.debug("Begin taping of the deletion of file {}", fileToTape.getName());
        tapingLock.lockForWriting();
        try {
            if (fileToTape.length() > 0) {
                log.debug("File {} containted content, so add the content before deletion", fileToTape.getName());
                tapeTheTapingFileAddition(fileToTape);
            }
            URI id = TapeUtils.getIdfromFile(fileToTape);
            log.debug("Taping the file deletion {} for real this time", fileToTape.getName());
            tapeArchive.remove(id);
            FileUtils.deleteQuietly(fileToTape);
        } finally {
            tapingLock.unlockForWriting();
        }
    }




    public boolean isTimerHaveRunAtLeastOnce() {
        return timerHaveRunAtLeastOnce;
    }

    public void setTapeDelay(long tapeDelay) {
        this.tapeDelay = tapeDelay;
    }
}
