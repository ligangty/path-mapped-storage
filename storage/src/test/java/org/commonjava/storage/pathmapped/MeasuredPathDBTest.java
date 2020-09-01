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
import org.commonjava.o11yphant.metrics.DefaultMetricRegistry;
import org.commonjava.o11yphant.metrics.DefaultMetricsManager;
import org.commonjava.storage.pathmapped.config.DefaultPathMappedStorageConfig;
import org.commonjava.storage.pathmapped.core.FileBasedPhysicalStore;
import org.commonjava.storage.pathmapped.core.PathMappedFileManager;
import org.commonjava.storage.pathmapped.metrics.MeasuredPathDB;
import org.commonjava.storage.pathmapped.pathdb.datastax.CassandraPathDB;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;
import static org.commonjava.o11yphant.metrics.util.MetricUtils.newDefaultMetricRegistry;
import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_HOST;
import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_KEYSPACE;
import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.PROP_CASSANDRA_PORT;

@Ignore
public class MeasuredPathDBTest
{
    private final String TEST_FS = "test";

    private final String PATH = "foo/bar/1.0/bar-1.0.pom";

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private PathMappedFileManager fileManager;

    private MeasuredPathDB measuredPathDB;

    @Before
    public void setup() throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Map<String, Object> props = new HashMap<>();
        props.put( PROP_CASSANDRA_HOST, "localhost" );
        props.put( PROP_CASSANDRA_PORT, 9142 );
        props.put( PROP_CASSANDRA_KEYSPACE, "indystorage" );

        DefaultPathMappedStorageConfig config = new DefaultPathMappedStorageConfig( props );
        // In test, we should let gc happened immediately when triggered.
        config.setGcGracePeriodInHours( 0 );
        config.setDeduplicatePattern( "^(generic|npm|test).*" );
        PathDB pathDB = new CassandraPathDB( config );

        DefaultMetricRegistry metricRegistry = newDefaultMetricRegistry();
        measuredPathDB = new MeasuredPathDB( pathDB, new DefaultMetricsManager( metricRegistry ), "pathDB" );

        File baseDir = temp.newFolder();
        fileManager = new PathMappedFileManager( config, measuredPathDB, new FileBasedPhysicalStore( baseDir ) );

        metricRegistry.startConsoleReporter( 3 );
    }

    @After
    public void shutdown()
    {
    }

    @Test
    public void run() throws Exception
    {
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, PATH ))
        {
            Assert.assertNotNull( os );
            IOUtils.write( "This is a test".getBytes(), os );
        }

        try (InputStream in = fileManager.openInputStream( TEST_FS, PATH ))
        {
            IOUtils.toString( in );
        }

        fileManager.getFileSystemContaining( Arrays.asList( TEST_FS ), PATH );

        fileManager.exists( TEST_FS, PATH );

        sleep( 10000 );

    }

}
