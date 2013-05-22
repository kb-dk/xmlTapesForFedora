package dk.statsbiblioteket.metadatarepository.xmltapes.interfaces;

import de.schlichtherle.truezip.file.TFile;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 11:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexMapImpl implements Index {


    private Map<URI,List<TFile>> locations;
    public IndexMapImpl() {

        locations = new HashMap<URI, List<TFile>>();
    }

    @Override
    public synchronized List<TFile> getLocations(URI id) {
        return locations.get(id);
    }

    @Override
    public synchronized void addLocation(URI id, TFile location) {
        List<TFile> knownLocations = locations.get(id);
        if (knownLocations == null){
            knownLocations = new ArrayList<TFile>(1);
        }
        knownLocations.add(location);
        locations.put(id,knownLocations);
    }

    @Override
    public synchronized void remove(URI id) {
        locations.remove(id);
    }

    @Override
    public synchronized Iterator<URI> getIds(String filterPrefix) {

        HashSet<URI> keys = new HashSet<URI>();
        for (URI uri : locations.keySet()) {
            if (uri.toString().startsWith(filterPrefix)){
                keys.add(uri);
            }
        }
        return keys.iterator();
    }


}
