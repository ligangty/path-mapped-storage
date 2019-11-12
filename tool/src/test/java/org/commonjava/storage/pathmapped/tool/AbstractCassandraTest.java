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
package org.commonjava.storage.pathmapped.tool;

import com.datastax.driver.core.Session;
import org.apache.commons.io.IOUtils;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.commonjava.storage.pathmapped.config.DefaultPathMappedStorageConfig;
import org.commonjava.storage.pathmapped.core.FileBasedPhysicalStore;
import org.commonjava.storage.pathmapped.core.PathMappedFileManager;
import org.commonjava.storage.pathmapped.datastax.CassandraPathDB;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_HOST;
import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_KEYSPACE;
import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_PORT;

public class AbstractCassandraTest
{
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

    void writeWithContent( String fileSystem, String path, String content )
    {
        try
        {
            writeWithContent( fileManager.openOutputStream( fileSystem, path ), content );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    static CassandraPathDB pathDB;

    PathMappedFileManager fileManager;

    private String baseStoragePath;

    static DefaultPathMappedStorageConfig config;

    static final String KEYSPACE = "test";

    static final String TEST_FS = "test";

    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Map<String, Object> props = new HashMap<>();
        props.put( PROP_CASSANDRA_HOST, "localhost" );
        props.put( PROP_CASSANDRA_PORT, 9142 );
        props.put( PROP_CASSANDRA_KEYSPACE, KEYSPACE );

        config = new DefaultPathMappedStorageConfig( props );
        // In test, we should let gc happened immediately when triggered.
        config.setGcGracePeriodInHours( 0 );
        pathDB = new CassandraPathDB( config );

    }

    @AfterClass
    public static void shutdown()
    {
        if ( pathDB != null )
        {
            pathDB.close();
        }
    }

    @Before
    public void setup() throws Exception
    {
        File baseDir = temp.newFolder();
        baseStoragePath = baseDir.getCanonicalPath();
        fileManager = new PathMappedFileManager( config, pathDB, new FileBasedPhysicalStore( baseDir ) );
    }

    @After
    public void teardown()
    {
        if ( pathDB != null )
        {
            Session session = pathDB.getSession();
            session.execute( "TRUNCATE " + KEYSPACE + ".pathmap;" );
            session.execute( "TRUNCATE " + KEYSPACE + ".reversemap;" );
            session.execute( "TRUNCATE " + KEYSPACE + ".reclaim;" );
            session.execute( "TRUNCATE " + KEYSPACE + ".filechecksum;" );
        }
    }

}
