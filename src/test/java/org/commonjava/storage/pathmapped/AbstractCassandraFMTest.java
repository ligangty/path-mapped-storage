/**
 * Copyright (C) 2013~2019 Red Hat, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.storage.pathmapped;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.commonjava.storage.pathmapped.config.DefaultPathMappedStorageConfig;
import org.commonjava.storage.pathmapped.core.FileBasedPhysicalStore;
import org.commonjava.storage.pathmapped.core.PathMappedFileManager;
import org.commonjava.storage.pathmapped.datastax.CassandraPathDB;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_HOST;
import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_KEYSPACE;
import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.PROP_CASSANDRA_PORT;

public abstract class AbstractCassandraFMTest
{

    static final int COUNT = 2000;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    private static CassandraPathDB pathDB;

    PathMappedFileManager fileManager;

    private static final String KEYSPACE = "test";

    static final String TEST_FS = "test";

    private String baseStoragePath;

    @BeforeClass
    public static void startEmbeddedCassandra()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Map<String, Object> props = new HashMap<>();
        props.put( PROP_CASSANDRA_HOST, "localhost" );
        props.put( PROP_CASSANDRA_PORT, 9142 );
        props.put( PROP_CASSANDRA_KEYSPACE, KEYSPACE );

        DefaultPathMappedStorageConfig config = new DefaultPathMappedStorageConfig( props );
        // In test, we should let gc happened immediately when triggered.
        config.setGcGracePeriodInHours( 0 );
        pathDB = new CassandraPathDB( config );

    }

    String getBaseDir()
    {
        return baseStoragePath;
    }

    @Before
    public void setup()
            throws Exception
    {
        File baseDir = temp.newFolder();
        baseStoragePath = baseDir.getCanonicalPath();
        fileManager = new PathMappedFileManager( new DefaultPathMappedStorageConfig(), pathDB,
                                                 new FileBasedPhysicalStore( baseDir ) );
    }

    @After
    public void teardown()
    {
        clearData();
        EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra( KEYSPACE );
    }

    protected void clearData()
    {
    }
}
