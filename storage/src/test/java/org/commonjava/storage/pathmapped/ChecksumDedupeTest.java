/**
 * Copyright (C) 2013~2019 Red Hat, Inc.
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

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ChecksumDedupeTest
        extends AbstractCassandraFMTest
{
    @Test
    public void checksumDupe()
            throws Exception
    {
        writeWithContent( TEST_FS, path1, simpleContent );
        writeWithContent( TEST_FS, path2, simpleContent );

        String pathStorage1 = fileManager.getFileStoragePath( TEST_FS, path1 );
        String pathStorage2 = fileManager.getFileStoragePath( TEST_FS, path2 );

        Assert.assertThat( pathStorage1, CoreMatchers.equalTo( pathStorage2 ) );

        checkPhysicalFile( 2, 1, pathStorage1 );

        checkRead( TEST_FS, path1, true, simpleContent );
        checkRead( TEST_FS, path2, true, simpleContent );

    }

    @Test
    public void checksumNotDupe()
            throws Exception
    {
        writeWithContent( TEST_FS, path1, simpleContent );
        final String newContent = simpleContent + "new";
        writeWithContent( TEST_FS, path2, newContent );

        final String pathStorage1 = fileManager.getFileStoragePath( TEST_FS, path1 );
        final String pathStorage2 = fileManager.getFileStoragePath( TEST_FS, path2 );

        Assert.assertThat( pathStorage1, CoreMatchers.not( pathStorage2 ) );

        checkPhysicalFile( 2, 2, pathStorage1, pathStorage2 );

        checkRead( TEST_FS, path1, true, simpleContent );
        checkRead( TEST_FS, path2, true, newContent );
    }

    @Test
    public void checksumDelete()
            throws Exception
    {
        writeWithContent( TEST_FS, path1, simpleContent );
        writeWithContent( TEST_FS, path2, simpleContent );
        final String path3 = pathParent+"/sub3/target3.txt";
        writeWithContent( TEST_FS, path3, simpleContent );

        final String pathStorage1 = fileManager.getFileStoragePath( TEST_FS, path1 );
        final String pathStorage2 = fileManager.getFileStoragePath( TEST_FS, path2 );
        final String pathStorage3 = fileManager.getFileStoragePath( TEST_FS, path3 );

        fileManager.delete( TEST_FS, path3 );
        checkPhysicalFile( 3, 1, pathStorage1 );
        checkRead( TEST_FS, path1, true, simpleContent );
        checkRead( TEST_FS, path2, true, simpleContent );
        checkRead( TEST_FS, path3, false, null );

        fileManager.delete( TEST_FS, path2 );
        checkPhysicalFile( 1, 1, pathStorage1 );
        checkRead( TEST_FS, path1, true, simpleContent );
        checkRead( TEST_FS, path2, false, null );
        checkRead( TEST_FS, path3, false, null );

        fileManager.delete( TEST_FS, path1 );
        checkPhysicalFile( 1, 0 );
        checkRead( TEST_FS, path1, false, null );
        checkRead( TEST_FS, path2, false, null );
        checkRead( TEST_FS, path3, false, null );

        writeWithContent( TEST_FS, path1, simpleContent );
        final String newPathStorage1 = fileManager.getFileStoragePath( TEST_FS, path1 );
        Assert.assertThat( newPathStorage1, CoreMatchers.not( pathStorage1));
        checkPhysicalFile( 1, 1, newPathStorage1 );
        checkRead( TEST_FS, path1, true, simpleContent );
        checkRead( TEST_FS, path2, false, null );
        checkRead( TEST_FS, path3, false, null );

    }

    private void checkPhysicalFile( final int filesBeforeGC, final int filesAfterGC, final String... pathsShouldCheck )
            throws Exception
    {
        checkFileCount( filesBeforeGC );

        triggerGC();

        Set<String> paths = checkFileCount( filesAfterGC );

        for ( String pc : pathsShouldCheck )
        {
            boolean matched = false;
            for ( String p : paths )
            {
                matched = p.contains( pc );
                if ( matched )
                {
                    break;
                }
            }
            if ( !matched )
            {
                Assert.fail( String.format( "Failed: not matched for %s in all paths %s", pc, paths ) );
            }
        }
    }

    private Set<String> checkFileCount( final int fileCount )
            throws IOException
    {
        Set<String> paths = new HashSet<>( fileCount );
        Files.walk( Paths.get( getBaseDir() ) )
             .filter( p -> Files.isRegularFile( p ) )
             .map( Path::toString )
             .forEach( paths::add );
        Assert.assertThat( paths.size(), CoreMatchers.equalTo( fileCount ) );
        return paths;
    }

    private void triggerGC()
            throws InterruptedException
    {
        fileManager.gc();
        Thread.sleep( GC_WAIT_MS );
    }

    private void checkRead( final String fileSys, final String path, final boolean exists,
                            final String expected )
            throws IOException
    {
        try (InputStream is = fileManager.openInputStream( fileSys, path ))
        {
            if ( exists )
            {
                Assert.assertThat( is, CoreMatchers.notNullValue() );
                Assert.assertThat( IOUtils.toString( is ), CoreMatchers.equalTo( expected ) );
            }
            else
            {
                Assert.assertThat( is, CoreMatchers.nullValue() );
            }
        }
    }
}
