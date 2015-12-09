package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import static org.junit.Assert.assertEquals;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchive2IT;

public class PostgresTapeArchive2IT extends AbstractTapeArchive2IT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }

}
