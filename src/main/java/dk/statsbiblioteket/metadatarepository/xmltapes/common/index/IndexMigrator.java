package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for migrating from one index to another 
 */
public class IndexMigrator {

    private static final Logger log = LoggerFactory.getLogger(IndexMigrator.class);
    
    private final Index src;
    private final Index dest;
    
    public IndexMigrator(Index src, Index dest) {
        this.src = src;
        this.dest = dest;
    }
    
    public void migrate() {
        long migratedEntries = 0;
        long tstart = System.currentTimeMillis();
        log.info("Starting migration");
        
        Set<String> indexedTapes = new HashSet<>();
        Iterator<URI> ids = src.listIds(null);
        
        while(ids.hasNext()) {
            URI currentID = ids.next();
            Entry entry = src.getLocation(currentID);
            File tape = entry.getTape();
            if(src.isIndexed(tape.getName())) {
                indexedTapes.add(tape.getName());
            }
            
            dest.addLocation(currentID, entry);
            migratedEntries++;
            if((migratedEntries % 10000) == 0) {
                log.info("Migrated 10000 entries in {}, total migrated entries {}", 
                        ((System.currentTimeMillis() - tstart)/1000), migratedEntries);
            }
        }
        long tmigrationFinish = System.currentTimeMillis();
        log.info("Finished migrating index. Migrated {} entries in {}s", 
                migratedEntries, ((tmigrationFinish - tstart) / 1000));
        
        log.info("Starting setting tapes as indexed");
        for(String tape : indexedTapes) {
            dest.setIndexed(tape);
        }
        long tstop = System.currentTimeMillis();
        log.info("Finished setting tapes as indexed. {} tapes marked as indexed in {}s ", 
                indexedTapes.size(), ((tstop - tmigrationFinish)/1000));
    }
    
}
