package dk.statsbiblioteket.metadatarepository.xmltapes.common;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 12:39 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Archive {


    /**
     * Open an inputstream to read the given blob
     * @param id the id of the blob
     * @return an inputstream to read from
     * @throws FileNotFoundException if the file was not found in the store
     */
    InputStream getInputStream(URI id) throws FileNotFoundException, IOException;

    /**
     * Check if the blob exists in the store
     * @param id the id to check
     * @return true of the blob is in the store
     */
    boolean exist(URI id) throws IOException;

    /**
     * Return the length of a blob
     * @param id the id of the blob
     * @return the length
     * @throws FileNotFoundException if the file was not found in the store
     * @throws IOException if the file could not be read from the archive
     */
    long getSize(URI id) throws FileNotFoundException, IOException;


    /**
     * List all Ids in the archive that adhere to a given interface
     * @param filterPrefix the blob id prefix
     * @return
     */
    Iterator<URI> listIds(String filterPrefix);





    /**
     * Initialise the archive
     * @throws IOException
     */
    public void init() throws IOException ;


    void close() throws IOException;


}


