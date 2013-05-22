package dk.statsbiblioteket.metadatarepository.xmltapes;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileInputStream;
import de.schlichtherle.truezip.file.TFileOutputStream;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.IndexMapImpl;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.StoreLock;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.TapeOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/17/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZipArchive implements Archive {


    private final StoreLock writeLock = new StoreLock();

    private final Index index;
    private final TFile zipfile;


    public ZipArchive(URI id) throws IOException {

        index = new IndexMapImpl();
        zipfile = new TFile(new File(id),TArchiveDetector.ALL);

    }

    @Override
    public String toFilename(URI id){
        return id.toString()+"@"+System.currentTimeMillis();
    }


    private TFile findFile(final URI id){
        List<TFile> files = index.getLocations(id);
        if (files != null){
        return files.get(files.size()-1);
        } else {
            return null;
        }
    }

    @Override
    public InputStream getInputStream(final URI id) throws FileNotFoundException {

            TFile newestFile = findFile(id);
            if (newestFile == null){
                throw new FileNotFoundException("File "+id.toString()+"not found");
            }

            return new TFileInputStream(newestFile);


    }

    @Override
    public boolean exist(URI id)  {

        return index.getLocations(id) != null;
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException {
            TFile newestFile = findFile(id);
            if (newestFile == null){
                throw new FileNotFoundException();
            }
            return newestFile.length();
    }

    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
            TFile toCreate = new TFile(zipfile, toFilename(id));
            if (toCreate.exists()){
                throw new RuntimeException();
            }
            toCreate.getParentFile().mkdirs();
            index.addLocation(id,toCreate);

            TFileOutputStream tFileOutputStream = new TFileOutputStream(toCreate, false);
            return new TapeOutputStream(tFileOutputStream,writeLock);



    }

    @Override
    public void removeFromIndex(URI id) {
        index.remove(id);
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return index.getIds(filterPrefix);
    }
}
