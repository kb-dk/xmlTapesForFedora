package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.StoreLock;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeInputStream;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeOutputStream;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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


/**
 * This class implements the Archive in the fashion of tar tape archived
 */
public class TapeArchive implements Archive {


    public static final String TAPE = "tape";
    public static final String TAR = ".tar";
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
    private final StoreLock writeLock = new StoreLock();

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
     * Instantiate a new Tape archive. This class should be a singleton, as the locking is controlled here
     *
     * @param location the folder with the archive files
     * @param tapeSize the cuttoff size to use for new tapes
     */
    public TapeArchive(URI location, long tapeSize) throws IOException {
        SIZE_LIMIT = tapeSize;
        archiveTapes = new File(location);
        File[] tapes = getTapes();

        if (tapes.length == 0) {
            newestTape = createNewTape();
        } else {
            newestTape = tapes[tapes.length - 1];
        }

    }


    /**
     * Clear the index and rebuild it
     *
     * @throws IOException
     */
    public void init() throws IOException {
        // Clear the index
        index.clear();
        File[] tapes = getTapes();
        // Iterate through all the tapes in sorted order to rebuild the index
        for (File tape : tapes) {

            // Create a TarInputStream
            TarInputStream tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tape)));
            TarEntry entry;

            long offset = 0;

            while ((entry = tis.getNextEntry()) != null) {
                URI id = TapeUtils.toURI(entry);
                if (entry.getSize() > 0) {
                    index.addLocation(id, new Entry(tape, offset));
                } else {
                    index.remove(id);
                }
                offset += entry.getSize();
            }
            tis.close();
        }

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
                return new Long(o1.lastModified()).compareTo(o2.lastModified());
            }
        });
        return tapes;
    }


    /**
     * Close the current tape and create a new one.
     *
     * @return the file ref to the new tape
     * @throws IOException
     */
    private synchronized File closeAndStartNewTape() throws IOException {
        //close tape
        TarOutputStream tarStream = new TarOutputStream(new FileOutputStream(newestTape, true));
        tarStream.close();

        //Create the new Tape
        return createNewTape();

    }

    /**
     * Create a new empty tar file. It is named after the timestamp where it was created
     *
     * @return a file ref to the new tar tape
     * @throws IOException
     */
    private synchronized File createNewTape() throws IOException {
        newestTape = new File(archiveTapes, TAPE + System.currentTimeMillis() + TAR);
        newestTape.createNewFile();
        return newestTape;
    }


    @Override
    public InputStream getInputStream(final URI id) throws IOException {

        Entry newestFile = index.getLocation(id);
        if (newestFile == null) {
            throw new FileNotFoundException("File " + id.toString() + "not found");
        }

        return getContentInputStream(newestFile);


    }

    /**
     * Get the inputstream corresponding to the entry, without the tar header
     *
     * @param entry the index entry
     * @return an inputstream to the data
     * @throws IOException
     */
    private InputStream getContentInputStream(Entry entry) throws IOException {
        TarInputStream tapeInputstream = getTarInputStream(entry);

        TarEntry tarEntry = tapeInputstream.getNextEntry();
        return new TapeInputStream(tapeInputstream, tarEntry.getSize());
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
                new BufferedInputStream(
                        new FileInputStream(
                                entry.getTape())));
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

        Entry newestFile = index.getLocation(id);
        if (newestFile == null) {
            throw new FileNotFoundException();
        }
        return getSize(newestFile);

    }

    private long getSize(Entry entry) throws IOException {
        InputStream inputStream = getTarInputStream(entry);
        return new TarInputStream(inputStream).getNextEntry().getSize();
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
    public synchronized OutputStream createNew(URI id, long estimatedSize) throws IOException {
        writeLock.lock(this); //Ensure that nobody will be writing when we calculate the size and start a new tape archive
        if (calculateTarSize(newestTape) > SIZE_LIMIT) {
            closeAndStartNewTape();
        }
        writeLock.unlock(this);
        Entry toCreate = new Entry(newestTape, newestTape.length());
        return new TapeOutputStream(new BufferedOutputStream(new FileOutputStream(newestTape, true)), toCreate, id, index, writeLock);


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
    public synchronized void remove(URI id) throws IOException {
        OutputStream outstream = createNew(id, 0);
        outstream.close();
        index.remove(id);
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return index.listIds(filterPrefix);
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }


}
