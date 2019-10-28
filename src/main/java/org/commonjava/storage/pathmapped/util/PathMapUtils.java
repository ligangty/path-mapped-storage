package org.commonjava.storage.pathmapped.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.commonjava.storage.pathmapped.model.PathMap;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.apache.commons.lang.StringUtils.isBlank;

public class PathMapUtils
{
    public final static String ROOT_DIR = "/";

    // looks weird but it splits "/path/to/my/file" -> [/][path/][to/][my/][file]
    private final static String EMPTY_CHAR_AFTER_SLASH = "(?<=/)";

    /**
     * Retrieve parent for the specified path. Prepend "/" if have not, and remove trailing "/" if have.
     */
    public static String getParentPath( String path )
    {
        if ( isRoot( path ) )
        {
            return null; // root have no parent path
        }

        StringBuilder sb = new StringBuilder();
        String[] toks = path.split( EMPTY_CHAR_AFTER_SLASH );
        for ( int i = 0; i < toks.length - 1; i++ )
        {
            sb.append( toks[i] );
        }
        String ret = sb.toString();
        return normalizeParentPath( ret );
    }

    /**
     * The specified path might be root, "org/", "/org/", and so on. In pathmapped table,
     * we always prepend a "/" to parentPath and remove the trailing "/". I.e, for anything
     * like "org/" or "/org/", the stored parentPath is "/org".
     */
    public static String normalizeParentPath( String path )
    {
        if ( isRoot( path ) )
        {
            return path;
        }
        if ( path.endsWith( "/" ) )
        {
            path = path.substring( 0, path.length() - 1 ); // remove trailing /
        }
        if ( !path.startsWith( "/" ) )
        {
            path = "/" + path;
        }
        return path;
    }

    public static boolean isRoot( String path )
    {
        return ROOT_DIR.equals( path );
    }

    public static String getFilename( String path )
    {
        if ( isRoot( path ) )
        {
            return null;
        }
        String[] toks = path.split( EMPTY_CHAR_AFTER_SLASH );
        return toks[toks.length - 1];
    }

    private static final int LEVEL_1_DIR_LENGTH = 2;

    private static final int LEVEL_2_DIR_LENGTH = 2;

    private static final int DIR_LENGTH = LEVEL_1_DIR_LENGTH + LEVEL_2_DIR_LENGTH;

    public static String getRandomFileId()
    {
        return UUID.randomUUID().toString();
    }

    public static String getStorageDir( String fileSystem, String path )
    {
        String uri = fileSystem + ":" + path;
        String md5Hex = DigestUtils.md5Hex( uri );
        String folder = md5Hex.substring( 0, LEVEL_1_DIR_LENGTH );
        String subFolder = md5Hex.substring( LEVEL_1_DIR_LENGTH, DIR_LENGTH );
        return folder + "/" + subFolder;
    }

    /**
     * Get parents starting in top-down order.
     */
    public static <R> List<R> getParents( PathMap pathMap,
                                             PathMapCreation<String, String, String, R> pathMapCreation )
    {
        String fileSystem = pathMap.getFileSystem();
        String parent = pathMap.getParentPath(); // e.g, /foo/bar/1.0

        LinkedList<R> l = new LinkedList<>();

        String parentPath = ROOT_DIR;

        String[] toks = parent.split( "/" ); // [][foo][bar][1.0]
        for ( String tok : toks )
        {
            if ( isBlank( tok ) )
            {
                continue;
            }
            String filename = tok + "/";
            R o = pathMapCreation.apply( fileSystem, parentPath, filename );
            l.add( o );
            if ( !parentPath.endsWith( "/" ) )
            {
                parentPath += "/";
            }
            parentPath += tok;
        }
        return l;
    }

    @FunctionalInterface
    public interface PathMapCreation<T1, T2, T3, R>
    {
        R apply( T1 fileSystem, T2 parentPath, T3 filename );
    }

    public static <R> List<R> getParentsBottomUp( PathMap pathMap,
                                                    PathMapCreation<String, String, String, R> pathMapCreation )
    {
        List<R> l = getParents( pathMap, pathMapCreation );
        Collections.reverse( l );
        return l;
    }

    // always prepend "/" to path
    public static String marshall( String fileSystem, String path )
    {
        if ( !path.startsWith( "/" ) )
        {
            path = "/" + path;
        }
        return fileSystem + ":" + path;
    }

    public static String normalize( final String... path )
    {
        if ( path == null || path.length < 1 || ( path.length == 1 && path[0] == null ) )
        {
            return ROOT_DIR;
        }

        final StringBuilder sb = new StringBuilder();
        int idx = 0;
        parts:
        for ( String part : path )
        {
            if ( part == null || part.length() < 1 || "/".equals( part ) )
            {
                continue;
            }
            if ( idx == 0 && part.startsWith( "file:" ) )
            {
                if ( part.length() > 5 )
                {
                    sb.append( part.substring( 5 ) );
                }
                continue;
            }
            if ( idx > 0 )
            {
                while ( part.charAt( 0 ) == '/' )
                {
                    if ( part.length() < 2 )
                    {
                        continue parts;
                    }
                    part = part.substring( 1 );
                }
            }
            while ( part.charAt( part.length() - 1 ) == '/' )
            {
                if ( part.length() < 2 )
                {
                    continue parts;
                }
                part = part.substring( 0, part.length() - 1 );
            }
            if ( sb.length() > 0 )
            {
                sb.append( '/' );
            }
            sb.append( part );
            idx++;
        }

        if ( path[path.length - 1] != null && path[path.length - 1].endsWith( "/" ) )
        {
            sb.append( "/" );
        }
        return sb.toString();
    }
}
