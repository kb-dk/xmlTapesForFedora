package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

import java.net.URI;
import java.util.Iterator;

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
        long indexedTapes = 0;
        long tstart = System.currentTimeMillis();
        long lap = tstart;
        log.info("Starting migration");
        
        Iterator<URI> ids = src.listIds(null);
        
        while(ids.hasNext()) {
            URI currentID = ids.next();
            Entry entry = src.getLocation(currentID);

            dest.addLocation(currentID, entry);
            migratedEntries++;
            if((migratedEntries % 10000) == 0) {
                log.info("Migrated 10000 entries in {}s, total migrated entries {}", 
                        ((System.currentTimeMillis() - lap)/1000), migratedEntries);
                lap = System.currentTimeMillis();
            }
        }
        long tmigrationFinish = System.currentTimeMillis();
        log.info("Finished migrating index. Migrated {} entries in {}s", 
                migratedEntries, ((tmigrationFinish - tstart) / 1000));
        
        log.info("Starting setting tapes as indexed");
        Iterator<String> tapes = src.listIndexedTapes();
        
        while(tapes.hasNext()) {
            String tape = tapes.next();
            dest.setIndexed(tape);
            indexedTapes++;
        }
        
        long tstop = System.currentTimeMillis();
        log.info("Finished setting tapes as indexed. {} tapes marked as indexed in {}s ", 
                indexedTapes, ((tstop - tmigrationFinish)/1000));
    }
    
}
