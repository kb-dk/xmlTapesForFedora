package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchiveIT;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;

public class PostgresTapeArchiveIT extends AbstractTapeArchiveIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }
}
