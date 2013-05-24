package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.StoreLock;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.TapeInputStream;import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.TapeOutputStream;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
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
import java.util.List;

public class ZipArchive implements Archive {


    private static final long SIZE_LIMIT = 20*1024;
    private static final long EOFSIZE = 1024;
    private static final long BLOCKSIZE = 10 * EOFSIZE;
    private final StoreLock writeLock = new StoreLock();

    private final Index index;
    private final File archiveTapes;
    private File newestTape;


    private synchronized File closeAndStartNewTape() throws IOException {

        //Idea:
        //Close the newest tape
        //Create a new tape

        //close tape
        TarOutputStream tarStream = new TarOutputStream(new FileOutputStream(newestTape, true));
        tarStream.close();

        //Create the new Tape


        return createNewTape();

    }

    private File createNewTape() throws IOException {
        newestTape = new File(archiveTapes, "tape" + System.currentTimeMillis() + ".tar");
        newestTape.createNewFile();
        return newestTape;
    }


    public ZipArchive(URI id) throws IOException {
        index = new RedisIndex("localhost",6379);
        index.clear();


        archiveTapes = new File(id);
        File[] tapes = archiveTapes.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith("tape") && name.endsWith(".tar")) {
                    return true;
                }
                return false;
            }
        }
        );
        Arrays.sort(tapes,new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return new Long(o2.lastModified()).compareTo(o1.lastModified());
            }
        });
        if (tapes.length == 0){
            newestTape = createNewTape();
        } else {
            newestTape = tapes[0];
        }

        for (File tape : tapes) {

            // Create a TarInputStream
            TarInputStream tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tape)));
            TarEntry entry;

            long offset = 0;

            while((entry = tis.getNextEntry()) != null) {
                index.addLocation(toURI(entry),new Entry(tape,entry.getName(),offset));
                offset += entry.getSize();
            }
            tis.close();
        }

    }

    private URI toURI(TarEntry entry) {
        return toURI(entry.getName());
    }







    public URI toURI(String filename)  {
        return URI.create(filename.substring(0,filename.lastIndexOf("#")));
    }

    @Override
    public String toFilename(URI id) {
        return id.toString() + "#" + System.currentTimeMillis();
    }


    private Entry findFile(final URI id) {


        List<Entry> files = index.getLocations(id);
        if (! files.isEmpty()) {
            return files.get(0);
        } else {
            return null;
        }
    }

    @Override
    public InputStream getInputStream(final URI id) throws IOException {

        Entry newestFile = findFile(id);
        if (newestFile == null) {
            throw new FileNotFoundException("File " + id.toString() + "not found");
        }

        return getInputStream(newestFile);



    }

    private InputStream getInputStream(Entry newestFile) throws IOException {
        TarInputStream tapeInputstream = new TarInputStream(new BufferedInputStream(new FileInputStream(newestFile.getTape())));
        tapeInputstream.skip(newestFile.getOffset());
        TarEntry entry = tapeInputstream.getNextEntry();
        return new TapeInputStream(tapeInputstream,entry.getSize());
    }

    @Override
    public boolean exist(URI id) {

        return ! index.getLocations(id).isEmpty();
    }

    @Override
    public long getSize(URI id) throws IOException {
        Entry newestFile = findFile(id);
        if (newestFile == null) {
            throw new FileNotFoundException();
        }
        return getSize(newestFile);

    }

    private long getSize(Entry entry) throws IOException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(entry.getTape()));
        inputStream.skip(entry.getOffset());
        return new TarInputStream(inputStream).getNextEntry().getSize();
    }

    @Override
    public synchronized OutputStream createNew(URI id, long estimatedSize) throws IOException {
        if (calculateTarSize(newestTape) > SIZE_LIMIT) {
            closeAndStartNewTape();
        }
        Entry toCreate = new Entry(newestTape, toFilename(id),newestTape.length());
        return new TapeOutputStream(new BufferedOutputStream(new FileOutputStream(newestTape,true)), toCreate,id,index, writeLock);


    }

    private long calculateTarSize(File newestTape) {
        long fileSize = newestTape.length();
        fileSize += EOFSIZE;
        long blocks = fileSize / BLOCKSIZE;
        if (fileSize % BLOCKSIZE > 0){
            blocks++;
        }
        return blocks*BLOCKSIZE;
    }

    @Override
    public void remove(URI id) {
        index.remove(id);
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return index.listIds(filterPrefix);
    }

    @Override
    public void sync() {}




}
