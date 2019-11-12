package org.commonjava.storage.pathmapped;

import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaPathKey;
import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaPathMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.commonjava.storage.pathmapped.util.PathMapUtils.getFilename;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getParentPath;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getParents;

public class PathMapUtilsTest
{

    @Test
    public void getParentsTest()
    {
        JpaPathKey key = new JpaPathKey( "http://foo.com", "/path/to/my", "file.txt" );
        JpaPathMap pathMap = new JpaPathMap();
        pathMap.setPathKey( key );

        List<JpaPathMap> l = getParents( pathMap, ( fSystem, pPath, fName ) -> {
            JpaPathMap p = new JpaPathMap();
            p.setPathKey( new JpaPathKey( fSystem, pPath, fName ) );
            return p;
        } );
        for ( JpaPathMap map : l )
        {
            System.out.println( ">> " + map.getPathKey() );
        }
        Assert.assertEquals( 3, l.size() );
    }

    @Test
    public void getParentPathTest()
    {
        String parent = getParentPath( "/path/to/file.txt" );
        Assert.assertEquals( parent, "/path/to" );

        parent = getParentPath( "/path/to/" );
        Assert.assertEquals( parent, "/path" );

        parent = getParentPath( "/path" );
        Assert.assertEquals( parent, "/" );

        parent = getParentPath( "/" );
        Assert.assertNull( parent );
    }

    @Test
    public void getFilenameTest()
    {
        String filename = getFilename( "/path/to/file.txt" );
        Assert.assertEquals( filename, "file.txt" );

        filename = getFilename( "/path/to/" );
        Assert.assertEquals( filename, "to/" );

        filename = getFilename( "/path/" );
        Assert.assertEquals( filename, "path/" );

        filename = getFilename( "/" );
        Assert.assertNull( filename );
    }

}
