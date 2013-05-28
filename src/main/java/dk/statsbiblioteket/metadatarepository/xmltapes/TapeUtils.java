package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.kamranzafar.jtar.TarEntry;

import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/27/13
 * Time: 4:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class TapeUtils {


    public static final String NAME_SEPARATOR = "#";

    public static URI toURI(TarEntry entry) {
        return toURI(entry.getName());
    }

    public static URI toURI(String filename)  {


        int endIndex = filename.lastIndexOf(NAME_SEPARATOR);
        if (endIndex < 0){
            endIndex = filename.length();
        }
        return URI.create(filename.substring(0, endIndex));
    }


    public static String toFilename(URI id) {
        return id.toString() + NAME_SEPARATOR + System.currentTimeMillis();
    }
}
