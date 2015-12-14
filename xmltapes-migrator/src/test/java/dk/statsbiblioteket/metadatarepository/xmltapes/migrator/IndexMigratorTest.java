package dk.statsbiblioteket.metadatarepository.xmltapes.migrator;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * Created by abr on 14-12-15.
 */
public class IndexMigratorTest {

    @Test
    public void testMigrate() throws Exception {

        Index indexFrom = mock(Index.class);

        String tape1 = "tape1";
        String tape2 = "tape2";
        String tape3 = "tape3";

        //List the entries in From
        URI blob1ID = URI.create("blob1");
        Entry entry1 = new Entry(new File(tape2), 0);

        URI blob2ID = URI.create("blob2");
        Entry entry2 = new Entry(new File(tape2), 10045);

        URI blob3ID = URI.create("blob3");
        Entry entry3 = new Entry(new File(tape3), 0);

        when(indexFrom.listIds(null)).thenReturn(Arrays.asList(blob1ID, blob2ID, blob3ID).iterator());
        when(indexFrom.getLocation(blob1ID)).thenReturn(entry1);
        when(indexFrom.getLocation(blob2ID)).thenReturn(entry2);
        when(indexFrom.getLocation(blob3ID)).thenReturn(entry3);
        when(indexFrom.listIndexedTapes()).thenReturn(Arrays.asList(tape1, tape2, tape3).iterator());

        Index indexTo = mock(Index.class);
        IndexMigrator migrator = new IndexMigrator(indexFrom, indexTo);
        migrator.migrate();

        verify(indexTo).addLocation(blob1ID,entry1);
        verify(indexTo).addLocation(blob2ID,entry2);
        verify(indexTo).addLocation(blob3ID,entry3);
        verify(indexTo).setIndexed(tape1);
        verify(indexTo).setIndexed(tape2);
        verify(indexTo).setIndexed(tape3);
        verifyNoMoreInteractions(indexTo);
    }
}