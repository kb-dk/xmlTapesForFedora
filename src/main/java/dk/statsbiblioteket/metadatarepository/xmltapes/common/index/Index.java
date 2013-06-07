package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;


import java.net.URI;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 11:26 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Index {


    /**
     * Return the TFile locatios of the given id.
     * @param id the id to look up
     * @return the location of the object
     */
    public Entry getLocation(URI id);

    /**
     * Add a new location (version) to a id
     * @param id the id to ammend
     * @param location the new location
     */
    public void addLocation(URI id, Entry location,long timestamp);

    /**
     * Remove an id and all associated locations from the index
     * @param id the id to remove
     */
    public void remove(URI id);

    /**
     * Get all IDs that have a given prefix
     * @param filterPrefix the prefix
     * @return a list of IDs, random order
     */
    public Iterator<URI> listIds(String filterPrefix);


    public boolean isIndexed(String tapename);
    public void setIndexed(String tapename);


    void clear();

    public long iterate(long startTimestamp);

    public Record getRecord(long iteratorKey);

    public List<Record> getRecords(long iteratorKey, int amount);
}
