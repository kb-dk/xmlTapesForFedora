package dk.statsbiblioteket.metadatarepository.xmltapes;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileInputStream;
import de.schlichtherle.truezip.file.TFileOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/17/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZipArchive {

    private  de.schlichtherle.truezip.file.TFile zipfile;

    private final Object writeLock = new Object();

    private final Map<URI,ReadLock> readLocks = new HashMap<URI,ReadLock>();


    public ZipArchive(URI id) throws IOException {

        zipfile = new TFile(new File(id),TArchiveDetector.ALL);

    }

    public String toFilename(URI id){
        return id.toString()+"@"+System.currentTimeMillis();
    }


    private TFile findFile(final URI id){
        TFile[] matchingFiles = zipfile.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith(id.toString())) {
                    return true;
                }

                return false;
            }
        });
        if (matchingFiles == null || matchingFiles.length == 0){
            return null;
        }
        Arrays.sort(matchingFiles,new Comparator<TFile>() {
            @Override
            public int compare(TFile o1, TFile o2) {
                return o2.getName().compareTo(o1.getName());
            }
        });
        TFile newestFile = matchingFiles[0];
        return newestFile;
    }

    public InputStream getInputStream(final URI id) throws IOException {
        ReadLock lock = readLock(id);
        try {

            TFile newestFile = findFile(id);
            if (newestFile == null){
                throw new FileNotFoundException("File "+id.toString()+"not found");
            }

            return new TFileInputStream(newestFile);

        }finally {
            releaseLock(lock);
        }
    }

    public boolean exist(URI id) throws IOException {
        try {
        InputStream stream = getInputStream(id);
        if (stream == null){
            return false;
        } else {
            stream.close();
            return true;
        }
        } catch (FileNotFoundException e){
            return false;
        }
    }

    public long getSize(URI id) {
        ReadLock lock = readLock(id);
        try {
            TFile newestFile = findFile(id);
            return newestFile.length();
        }finally {
            releaseLock(lock);
        }
    }

    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        ReadLock lock = readLock(id);
        try {
            TFile toCreate = new TFile(zipfile, toFilename(id));
            if (toCreate.exists()){
                throw new RuntimeException();
            }
            toCreate.getParentFile().mkdirs();
            return new TFileOutputStream(toCreate,false);

        }finally {
            releaseLock(lock);
        }

    }

    public void removeFromIndex(URI id) {
        //To change body of created methods use File | Settings | File Templates.
    }

    public  ReadLock readLock(URI... ids) {
        ReadLock myLock = new ReadLock(ids);

        HashSet<URI> toLock = new HashSet<URI>(Arrays.asList(ids));
        while (!toLock.isEmpty()){
            HashSet<URI> locked = new HashSet<URI>();
            for (URI uri : toLock) {
                synchronized (readLocks){
                    if ( ! readLocks.containsKey(uri)){
                        readLocks.put(uri,myLock);
                        locked.add(uri);
                    }
                }
            }
            for (URI uri : locked) {
                toLock.remove(uri);
            }
        }
        return myLock;
    }


    public synchronized ReadLock writeLock(URI... ids) {

        return new ReadLock(ids);
    }

    public void releaseLock(ReadLock lock) {
        for (URI id : lock.getIds()) {
            synchronized (readLocks){
                if (readLocks.get(id) == lock){
                    readLocks.remove(id);
                }
            }
        }
    }

    public Iterator<URI> listIds(String filterPrefix) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }
}
