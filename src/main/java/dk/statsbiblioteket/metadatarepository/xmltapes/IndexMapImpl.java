package dk.statsbiblioteket.metadatarepository.xmltapes;

import de.schlichtherle.truezip.file.TFile;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 11:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexMapImpl implements Index {


    private Map<URI,SortedSet<TFile>> locations;
    public IndexMapImpl() {

        locations = new HashMap<URI, SortedSet<TFile>>();
    }

    @Override
    public synchronized SortedSet<TFile> getLocations(URI id) {
        return locations.get(id);
    }

    @Override
    public synchronized void addLocation(URI id, TFile location) {
        SortedSet<TFile> knownLocations = locations.get(id);
        if (knownLocations == null){
            knownLocations = new TreeSet<TFile>(new Comparator<TFile>() {
                @Override
                public int compare(TFile o1, TFile o2) {
                    return o2.getName().compareTo(o1.getName());
                }
            });
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
