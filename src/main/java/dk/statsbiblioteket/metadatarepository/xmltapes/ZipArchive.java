package dk.statsbiblioteket.metadatarepository.xmltapes;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileInputStream;
import de.schlichtherle.truezip.file.TFileOutputStream;
import de.schlichtherle.truezip.file.TVFS;
import de.schlichtherle.truezip.fs.FsSyncException;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.StoreLock;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.TapeOutputStream;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/17/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZipArchive implements Archive {


    private static final long SIZE_LIMIT = 20*1024;
    private final StoreLock writeLock = new StoreLock();

    private final Index index;
    private final TFile archiveTapes;
    private TFile newestTape;


    private TFile addNewTape() {
        return new TFile(archiveTapes, "tape" + System.currentTimeMillis() + ".tar");
    }


    public ZipArchive(URI id) throws IOException {
        index = new IndexMapImpl();

        archiveTapes = new TFile(new File(id), TArchiveDetector.ALL);
        TFile[] tapes = archiveTapes.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith("tape") && name.endsWith(".tar")) {
                    return true;
                }
                return false;
            }
        }
        );
        Arrays.sort(tapes,new Comparator<TFile>() {
            @Override
            public int compare(TFile o1, TFile o2) {
                return new Long(o2.lastModified()).compareTo(o1.lastModified());
            }
        });
        if (tapes.length == 0){
            newestTape = addNewTape();
        } else {
            newestTape = tapes[0];
        }

        for (TFile tape : tapes) {
            TFile[] objects = tape.listFiles();
            for (TFile object : objects) {
                index.addLocation(toURI(object.getName()),object);
            }
        }

    }

    public URI toURI(String filename)  {
       return URI.create(filename.substring(0,filename.lastIndexOf("@")));
    }

    @Override
    public String toFilename(URI id) {
        return id.toString() + "@" + System.currentTimeMillis();
    }


    private TFile findFile(final URI id) {
        SortedSet<TFile> files = index.getLocations(id);
        if (files != null) {
            return files.first();
        } else {
            return null;
        }
    }

    @Override
    public InputStream getInputStream(final URI id) throws FileNotFoundException {

        TFile newestFile = findFile(id);
        if (newestFile == null) {
            throw new FileNotFoundException("File " + id.toString() + "not found");
        }

        return new TFileInputStream(newestFile);


    }

    @Override
    public boolean exist(URI id) {

        return index.getLocations(id) != null;
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException {
        TFile newestFile = findFile(id);
        if (newestFile == null) {
            throw new FileNotFoundException();
        }
        return newestFile.length();
    }

    @Override
    public synchronized OutputStream createNew(URI id, long estimatedSize) throws IOException {
        if (new File(newestTape.getAbsolutePath()).length() > SIZE_LIMIT) {
            newestTape = addNewTape();
        }
        TFile toCreate = new TFile(newestTape, toFilename(id));
        if (toCreate.exists()) {
            throw new RuntimeException();
        }
        toCreate.getParentFile().mkdirs();
        index.addLocation(id, toCreate);

        TFileOutputStream tFileOutputStream = new TFileOutputStream(toCreate, false);
        return new TapeOutputStream(tFileOutputStream, writeLock);


    }

    @Override
    public void removeFromIndex(URI id) {
        index.remove(id);
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return index.getIds(filterPrefix);
    }

    @Override
    public void sync() throws FsSyncException {
        //TODO look more into how this syncs
        TVFS.sync();
    }
}
