package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.util.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/6/13
 * Time: 12:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class Hasher {

    public static String getHash(String uri) {
        try {
            return Bytes.toHex(MessageDigest.getInstance("MD5").digest(uri.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

}
