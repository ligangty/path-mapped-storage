/**
 * Copyright (C) 2019 Red Hat, Inc. (nos-devel@redhat.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
