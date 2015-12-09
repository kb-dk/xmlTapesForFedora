package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesBlobStoreIT;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class PostgresXmlTapesBlobStoreIT extends AbstractXmlTapesBlobStoreIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }

}
