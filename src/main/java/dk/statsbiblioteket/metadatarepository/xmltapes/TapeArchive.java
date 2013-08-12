package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeOutputStream;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.CountingOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class implements the Archive in the fashion of tar tape archived
 */
public class TapeArchive implements Archive {


    private static final Logger log = LoggerFactory.getLogger(TapeArchive.class);


    public static final String TAPE = "tape";
    public static final String TAR = ".tar.gz";
    public static final String TEMP_TAPE = "tempTape";


    /**
     * The max size a tarball can grow to, before we start a new tape.
     */
    private final long SIZE_LIMIT;

    /**
     * The size of a record
     */
    private static final long RECORDSIZE = 512;

    /**
     * The size of the two final records
     */
    private static final long EOFSIZE = 2 * RECORDSIZE;

    /**
     * Each block is 20 records
     */
    private static final long BLOCKSIZE = 20 * RECORDSIZE;

    /**
     * The store lock, a singleton
     */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * The folder with the archive tapes
     */
    private final File archiveTapes;

    /**
     * The "HEAD" tape
     */
    private File newestTape;

    /**
     * A reference to the index system
     */
    private Index index;

    /**
     * Verify that the newest tape can be read, and fix it if it cannot
     */
    private boolean fixErrors = false;






    /*-----------------STARTUP and REBUILDING-------------------------*/

    /**
     * Instantiate a new Tape archive. This class should be a singleton, as the locking is controlled here
     *
     * @param location the folder with the archive files
     * @param tapeSize the cuttoff size to use for new tapes
     */
    public TapeArchive(URI location, long tapeSize) throws IOException {

        log.info("Initialising tape archive from {}",location);
        SIZE_LIMIT = tapeSize;
        archiveTapes = new File(location);
        if ( !archiveTapes.exists()){
            archiveTapes.mkdirs();
        }
        if (!archiveTapes.isDirectory()){
            throw new IOException("Archive folder "+archiveTapes+" is not a directory");
        }
        File[] tapes = getTapes();

        if (tapes.length == 0) {
            newestTape = createNewTape();
        } else {
            newestTape = tapes[tapes.length - 1];
        }
        log.debug("Newest tape is {}",newestTape);

    }

    /**
     * Initialise the system.
     * Find the "HEAD" tar, scan it for errors, and add all from in to the index again
     * Check that all the tapes have been indexed
     */
    public void init() throws IOException {
        log.debug("Init called");
        File[] tapes = getTapes();
        if (isFixErrors()){ //Only fixErrors if explicitly told to
            verifyAndFix(newestTape); //Only check and fix the newest tape
        }
        // Iterate through all the tapes in sorted order to rebuild the index
        rebuildWhatsNeeded(tapes);


    }


    /**
     * Verify and fix the specified tape.
     * The verification is done by reading through the tape. If all records can be read without IOExceptions, the
     * tape is regarded as valid.
     * If an IOException occurs, the tape is copied, record for record, to a new temp tape. This copy will, of course,
     * stop when the IOException occurs. The invalid tape is then replaced with the temp tape, so the defect record
     * and all following records are removed.
     * @param tape the tape to verify and fix
     * @throws IOException If writing the new tape failed.
     */
    private void verifyAndFix(File tape) throws IOException {
        log.info("Verifying and fixing the content of the tape {}",tape);

        TarInputStream tarstream = new TarInputStream(new FileInputStream(tape));

        TarOutputStream tarout = null;

        TarEntry failedEntry = null;
        //verify
        try {

            while ((failedEntry = tarstream.getNextEntry()) != null) {
            }
            tarstream.close();
            log.info("File {} verified correctly",tape);
        } catch (IOException e) {
            //verification failed, read what we can and write it back
            //failedEntry should be the one that failed
            log.warn("Caught exception {}",e);
            log.warn("Failed to verify {}. I will now copy all that can be read to new file and replace the broken tape",
                    tape);

            File tempTape = File.createTempFile(TEMP_TAPE, TAR);
            tempTape.deleteOnExit();
            tarout = new TarOutputStream(new FileOutputStream(tempTape));
            //close and reopen
            IOUtils.closeQuietly(tarstream);
            tarstream = new TarInputStream(new FileInputStream(tape));

            //fix
            TarEntry entry;
            while ((entry = tarstream.getNextEntry()) != null) {
                if (equals(failedEntry, entry)) {
                    break;
                }
                tarout.putNextEntry(entry);
                IOUtils.copyLarge(tarstream, tarout, 0, entry.getSize());
            }
            IOUtils.closeQuietly(tarstream);
            IOUtils.closeQuietly(tarout);

            //move existing out of the way
            File temp2 = File.createTempFile(TEMP_TAPE, TAR);
            temp2.deleteOnExit();

            FileUtils.moveFile(tape, temp2);
            FileUtils.moveFile(tempTape, tape);
            FileUtils.deleteQuietly(temp2);
            log.info("The broken tape {} have now been replaced with what could be recovered.",tape);

            //And since we close the fixed tape now, we create a new one to hold further stuff
            createNewTape();

        } finally {
            IOUtils.closeQuietly(tarstream);
            IOUtils.closeQuietly(tarout);
        }
    }

    private boolean equals(TarEntry failedEntry, TarEntry entry) {
        return failedEntry.getName().equals(entry.getName())
                &&
                failedEntry.getSize() == entry.getSize();

    }


    /**
     * Clear the index and rebuild it
     *
     * @throws IOException
     */
    public void rebuild() throws IOException {
        log.info("The index should be rebuild, so clearing it");
        // Clear the index and then rebuild it
        index.clear();
        init();
    }

    @Override
    public void close() throws IOException {
        getStoreWriteLock();
        try {

        } finally {
            writeLock.unlock();
        }
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Iterate through the tapes and index them, if they are not already indexed. The newest tape will always be indexed
     * but will never be marked as Indexed
     * @param tapes the tapes, sorted from oldest to newest
     * @throws IOException if reading a tape failed
     */
    private void rebuildWhatsNeeded(File[] tapes) throws IOException {
        boolean indexedSoFar = true;

        //Iterate through all but the newest tape
        for (int i = 0; i < tapes.length-1; i++) {
            File tape = tapes[i];
            if (indexedSoFar && index.isIndexed(tape.getName())) {
                log.debug("File {} have already been indexed so is skipped",tape);
            } else {
                log.debug("File {} should be reindexed",tape);
                indexedSoFar = false;
                indexTape(tape);
                index.setIndexed(tape.getName());
                log.info("File {} have been indexed", tape);
            }
        }
        log.debug("File {} should be reindexed",newestTape);
        //Index the newest tape, but do not mark it as indexed
        indexTape(newestTape);
        log.debug("File {} have been indexed",newestTape);
    }

    /**
     * Index the tape
     * @param tape the tape to index
     * @throws IOException
     */
    private  void indexTape(File tape) throws IOException {
        // Create a TarInputStream
        CountingInputStream countingInputStream = new CountingInputStream(new BufferedInputStream(new FileInputStream(tape)));
        TarInputStream tis = new TarInputStream(countingInputStream);

        long offset = 0;

        TarEntry entry = tis.getNextEntry();
        while (entry != null) {
            URI id = TapeUtils.toURI(entry);
            long timestamp = TapeUtils.getTimestamp(entry);
            if (entry.getSize() == 0 && entry.getName().endsWith(TapeUtils.NAME_SEPARATOR+TapeUtils.DELETED)) {
                index.remove(id);
            } else {
                index.addLocation(id, new Entry(tape, offset));
            }
            entry = tis.getNextEntry();
            offset = countingInputStream.getByteCount()-RECORDSIZE;
        }
        tis.close();

    }



    /**
     * Get all the tapes in sorted order
     *
     * @return the list of tape files
     */
    private File[] getTapes() {
        //TODO should we do this twice or store the result?
        // Find all the tapes in the tape folder
        File[] tapes = archiveTapes.listFiles(
                new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.startsWith(TAPE) && name.endsWith(TAR)) {
                            return true;
                        }
                        return false;
                    }
                }
        );
        // Sort the files so that the oldest is first
        Arrays.sort(tapes, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return tapes;
    }












    /*-------------------------WORKING WITH TAPES-----------------------------*/









    /**
     * Close the current tape and create a new one.
     *
     * @return the file ref to the new tape
     * @throws IOException
     */
    private  File closeAndStartNewTape() throws IOException {
        getStoreWriteLock();
        try {
            log.debug("Closing the tape {} and starting a new one",newestTape);
            //close tape
            TarOutputStream tarStream = new TarOutputStream(new FileOutputStream(newestTape, true));
            tarStream.close();
            index.setIndexed(newestTape.getName());

            //Create the new Tape
            return createNewTape();
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Create a new empty tar file. It is named after the timestamp where it was created
     *
     * @return a file ref to the new tar tape
     * @throws IOException
     */
    private  File createNewTape() throws IOException {
        getStoreWriteLock();
        try {
            newestTape = new File(archiveTapes, TAPE + System.currentTimeMillis() + TAR);
            newestTape.createNewFile();
            log.debug("Starting the new tape {}",newestTape);
            return newestTape;
        } finally {
            writeLock.unlock();
        }

    }


    @Override
    public InputStream getInputStream(final URI id) throws IOException {
        log.debug("Calling getInputStream for id {}",id);
        Entry newestFile = index.getLocation(id);
        if (newestFile == null) {
            throw new FileNotFoundException("File " + id.toString() + "not found");
        }

        return getContentInputStream(newestFile, id);


    }

    /**
     * Get the inputstream corresponding to the entry, without the tar header
     *
     * @param entry the index entry
     * @return an inputstream to the data
     * @throws IOException
     */
    private InputStream getContentInputStream(Entry entry, URI id) throws IOException {
        TarInputStream tapeInputstream = getTarInputStream(entry);

        TarEntry tarEntry = tapeInputstream.getNextEntry();
        if (tarEntry != null && tarEntry.getName().startsWith(id.toString())){
            if (tarEntry.getName().endsWith(TapeUtils.GZ)){
                return new GzipCompressorInputStream(tapeInputstream);
            }
            return tapeInputstream;
        } else {
            log.warn("Could not find entry {} for id {}, instead found {}",new Object[]{entry,id,tarEntry.getName()});
            throw new IOException("Could not find entry in archive file");
        }
    }

    /**
     * Get the tar input stream corresponding to an entry. The first bytes in this stream will be the tar header
     *
     * @param entry
     * @return
     * @throws IOException
     */
    private TarInputStream getTarInputStream(Entry entry) throws IOException {
        TarInputStream tapeInputstream = new TarInputStream(
                new FileInputStream(
                        entry.getTape()));
        tapeInputstream.setDefaultSkip(true);
        long skipped = 0;
        while (skipped < entry.getOffset()) {
            skipped += tapeInputstream.skip(entry.getOffset() - skipped);
        }
        return tapeInputstream;
    }

    @Override
    public boolean exist(URI id) {

        return index.getLocation(id) != null;
    }

    @Override
    public long getSize(URI id) throws IOException {
        log.debug("Calling getSize for id {}",id);

        Entry newestFile = index.getLocation(id);
        if (newestFile == null) {
            throw new FileNotFoundException();
        }
        return getSize(newestFile);

    }

    private long getSize(Entry entry) throws IOException {
        TarInputStream inputStream = getTarInputStream(entry);
        inputStream.getNextEntry();
        GzipCompressorInputStream zipped = new GzipCompressorInputStream(inputStream);
        CountingOutputStream out = new CountingOutputStream(new NullOutputStream());
        IOUtils.copyLarge(zipped,out);
        return out.getBytesWritten();
    }

    /**
     * Creates a new entry in the tape archive.
     * Attempts to acquire the writelock, so any outstanding write operations
     * will have to be completed before the tape can be closed.
     *
     * @param id            the id of the new blob
     * @param estimatedSize the estimated size of the content. Ignored
     * @return
     * @throws IOException
     */
    @Override
    public  OutputStream createNew(URI id, long estimatedSize) throws IOException {
        getStoreWriteLock();
        log.debug("Calling createNew with id {}",id);
        if (calculateTarSize(newestTape) > SIZE_LIMIT) {
            closeAndStartNewTape();
        }

        Entry toCreate = new Entry(newestTape, newestTape.length());
        return new TapeOutputStream(
                new FileOutputStream(newestTape, true),
                toCreate,
                id,
                index,
                writeLock,
                estimatedSize);


    }


    private long calculateTarSize(File newestTape) {

        long fileSize = newestTape.length();
        fileSize += EOFSIZE;
        long blocks = fileSize / BLOCKSIZE;
        if (fileSize % BLOCKSIZE > 0) {
            blocks++;
        }
        return blocks * BLOCKSIZE;

    }

    @Override
    public  void remove(URI id) throws IOException {
        getStoreWriteLock();
        log.debug("Removing id {}",id);

        Entry newestFile = index.getLocation(id);
        if (newestFile == null) { //No reason to delete a file that does not exist in the index.
            return;
        }

        if (calculateTarSize(newestTape) > SIZE_LIMIT) {
            closeAndStartNewTape();
        }
        Entry toCreate = new Entry(newestTape, newestTape.length());

        TapeOutputStream out = new TapeOutputStream(
                new FileOutputStream(newestTape, true)
                ,
                toCreate,
                id,
                index,
                writeLock, 0);
        out.delete();

    }

    private void getStoreWriteLock() {
        while (true){
            try {
                if (writeLock.tryLock(10, TimeUnit.MILLISECONDS)){
                    break;
                }
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }

    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        log.debug("Calling listIds with prefix {}",filterPrefix);

        return index.listIds(filterPrefix);
    }





    /*--------------GETTERS AND SETTERS----------------------------*/



    public void setIndex(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }


    public boolean isFixErrors() {
        return fixErrors;
    }

    public void setFixErrors(boolean fixErrors) {
        this.fixErrors = fixErrors;
    }
}
