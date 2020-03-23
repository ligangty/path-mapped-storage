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

import org.apache.commons.io.IOUtils;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.commonjava.storage.pathmapped.config.DefaultPathMappedStorageConfig;
import org.commonjava.storage.pathmapped.core.FileBasedPhysicalStore;
import org.commonjava.storage.pathmapped.core.PathMappedFileManager;
import org.commonjava.storage.pathmapped.pathdb.datastax.CassandraPathDB;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_HOST;
import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_KEYSPACE;
import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_PORT;

@Ignore
public class QueryByPathPerformanceTest
{
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private PathMappedFileManager fileManager;

    @Before
    public void setup() throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Map<String, Object> props = new HashMap<>();
        props.put( PROP_CASSANDRA_HOST, "localhost" );
        props.put( PROP_CASSANDRA_PORT, 9142 );
        props.put( PROP_CASSANDRA_KEYSPACE, "indystorageperf" );

        DefaultPathMappedStorageConfig config = new DefaultPathMappedStorageConfig( props );
        PathDB pathDB = new CassandraPathDB( config );

        File baseDir = temp.newFolder();
        fileManager = new PathMappedFileManager( config, pathDB, new FileBasedPhysicalStore( baseDir ) );
    }


    final String repo = "maven:hosted:build-%s";
    final String path = "foo/bar/1.0/bar-1.0-%s.xml";

    // use another path to introduce more entries
    final String anotherPath =
                    "my/very/very/very/long/path/to/introduce/more/entries/for/get/file/system/containing/performance/test/foo/bar/1.0/bar-1.0-%s.xml";

    String noneExistDirPath = "none/exist/";

    List<String> candidates = new ArrayList<>();

    StringBuilder sb = new StringBuilder();

    /**
     * We set 3000 file systems each has a different path. And we do:
     * 1. search for the path of the 1500th file system
     * 2. search for a none existing path
     * 3. search for a none existing directory
     *
     * Result:
     * 1. getFileSystemContaining is 3 times faster than for-loop (351 vs 904)
     * 2. getFileSystemContaining is 4 times faster than for-loop (302 vs 1409)
     * 3. getFileSystemContaining is 6 times faster than for-loop (280 vs 1617)
     *
     * Sample output:
     *
     * ------- getFileSystemContaining ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (351)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (302)
     * none/exist/ (280)
     * ------- for loop ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (904)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (1409)
     * none/exist/, not exist, (1617)
     */
    @Test
    public void performance() throws IOException
    {
        // prepare
        for ( int i = 0; i < 3000; i++ )
        {
            String simpleContent = "This is a test " + UUID.randomUUID().toString();

            String repoName = String.format( repo, i );
            writeWithContent( fileManager.openOutputStream( repoName, String.format( path, i ) ), simpleContent );
            writeWithContent( fileManager.openOutputStream( repoName, String.format( anotherPath, i ) ),
                              simpleContent );
            candidates.add( repoName );
        }

        getFileSystemContainingTest();

        forLoopTest();

        // print all
        System.out.println( sb.toString() );
    }

    private void forLoopTest()
    {
        printIt( sb, "------- for loop ---------" );

        // exist
        boolean exist = false;
        String targetPath = String.format( path, 1500 );
        long begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, targetPath );
            if ( exist )
            {
                break;
            }
        }
        long elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // none exist
        targetPath = String.format( path, 99999 );
        begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, targetPath );
            if ( exist )
            {
                break;
            }
        }
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // dir
        begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, noneExistDirPath );
            if ( exist )
            {
                break;
            }
        }
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, noneExistDirPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );
    }

    private void getFileSystemContainingTest()
    {
        printIt( sb, "------- getFileSystemContaining ---------" );

        // exist
        String targetPath = String.format( path, 1500 );
        long begin = System.currentTimeMillis();
        Set<String> ret = fileManager.getFileSystemContaining( candidates, targetPath );
        long elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( !ret.isEmpty() ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // not exist
        targetPath = String.format( path, 99999 );
        begin = System.currentTimeMillis();
        ret = fileManager.getFileSystemContaining( candidates, targetPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( !ret.isEmpty() ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // dir
        begin = System.currentTimeMillis();
        ret = fileManager.getFileSystemContainingDirectory( candidates, noneExistDirPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, noneExistDirPath + " (" + elapse + ")" );
    }

    private void printIt( StringBuilder sb, String s )
    {
        sb.append( s + "\n" );
    }

    void writeWithContent( OutputStream stream, String content )
    {
        try (OutputStream os = stream)
        {
            IOUtils.write( content.getBytes(), os );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

}
