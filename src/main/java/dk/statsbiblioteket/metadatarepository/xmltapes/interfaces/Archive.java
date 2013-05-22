package dk.statsbiblioteket.metadatarepository.xmltapes.interfaces;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
     * Convert the URI identifier to a valid unique filename
     * @param id the uri
     * @return a valid filename
     */
    String toFilename(URI id);

    /**
     * Open an inputstream to read the given blob
     * @param id the id of the blob
     * @return an inputstream to read from
     * @throws FileNotFoundException if the file was not found in the store
     */
    InputStream getInputStream(URI id) throws FileNotFoundException;

    /**
     * Check if the blob exists in the store
     * @param id the id to check
     * @return true of the blob is in the store
     */
    boolean exist(URI id);

    /**
     * Return the length of a blob
     * @param id the id of the blob
     * @return the length
     * @throws FileNotFoundException if the file was not found in the store
     */
    long getSize(URI id) throws FileNotFoundException;

    /**
     * Create a new blob and open an outputstream to populate it
     * @param id the id of the new blob
     * @param estimatedSize the estimated size of the content
     * @return an outputstream to the new blob
     * @throws IOException if creation of a new blob failed
     */
    OutputStream createNew(URI id, long estimatedSize) throws IOException;


    /**
     * Remove the blob from the index, so that it will not be in the archive anymore
     * @param id the id to remove
     */
    void removeFromIndex(URI id);


    /**
     * List all Ids in the archive that adhere to a given interface
     * @param filterPrefix the blob id prefix
     * @return
     */
    Iterator<URI> listIds(String filterPrefix);
}
