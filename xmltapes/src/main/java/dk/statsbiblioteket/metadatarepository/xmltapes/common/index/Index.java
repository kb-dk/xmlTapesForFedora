package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;


import java.net.URI;
import java.util.Iterator;

public interface Index {


    /**
     * Return the TFile locatios of the given id.
     * @param id the id to look up
     * @return the location of the object
     */
    public Entry getLocation(URI id);

    /**
     * Add a new location (version) to a id.
     * @param id the id to ammend
     * @param location the new location
     */
    public void addLocation(URI id, Entry location);

    /**
     * Remove an id and all associated locations from the index.
     * @param id the id to remove
     */
    public void remove(URI id);

    /**
     * Get all IDs that have a given prefix.
     * @param filterPrefix the prefix
     * @return a list of IDs, random order
     */
    public Iterator<URI> listIds(String filterPrefix);


    /**
     * Determine if a tape has been indexed.
     * @param tapename The name of the tape
     * @return if the tape has been indexed 
     */
    public boolean isIndexed(String tapename);
    
    /**
     * Mark a tape as indexed.
     * @param tapename The name of the tape 
     */
    public void setIndexed(String tapename);

    /**
     * Get all IDs of tapes that have been indexed.
     * @return an iterator of IDs of all indexed tapes 
     */
    public Iterator<String> listIndexedTapes();

    /**
     * Empty the database 
     */
    void clear();
}
