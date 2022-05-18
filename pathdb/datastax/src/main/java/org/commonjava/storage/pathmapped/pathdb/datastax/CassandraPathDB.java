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
package org.commonjava.storage.pathmapped.pathdb.datastax;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import com.google.common.collect.TreeTraverser;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxFileChecksum;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxPathMap;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxReclaim;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxReverseMap;
import org.commonjava.storage.pathmapped.pathdb.datastax.util.AsyncJobExecutor;
import org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils;
import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.commonjava.storage.pathmapped.model.FileChecksum;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.model.ReverseMap;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.util.PathMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static java.util.Collections.emptySet;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.commonjava.storage.pathmapped.spi.PathDB.FileType.all;
import static org.commonjava.storage.pathmapped.spi.PathDB.FileType.dir;
import static org.commonjava.storage.pathmapped.spi.PathDB.FileType.file;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.ROOT_DIR;

public class CassandraPathDB
                implements PathDB, Closeable
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private AsyncJobExecutor asyncJobExecutor; // run non-critical jobs on backend

    private Session session;

    private Cluster cluster;

    private Mapper<DtxPathMap> pathMapMapper;

    private Mapper<DtxReverseMap> reverseMapMapper;

    private Mapper<DtxReclaim> reclaimMapper;

    private Mapper<DtxFileChecksum> fileChecksumMapper;

    private PathMappedStorageConfig config;

    private final String keyspace;

    private int replicationFactor = 1; // keyspace replica, default 1

    private PreparedStatement preparedExistQuery, preparedListQuery, preparedListCheckEmpty, preparedContainingQuery, preparedExistFileQuery,
                    preparedReverseMapIncrement, preparedReverseMapReduction;

    @Deprecated
    public CassandraPathDB( PathMappedStorageConfig config, Session session, String keyspace )
    {
        this( config, session, keyspace, 1 );
    }

    public CassandraPathDB( PathMappedStorageConfig config, Session session, String keyspace, int replicationFactor )
    {
        this.config = config;
        this.keyspace = keyspace;
        this.session = session;
        this.replicationFactor = replicationFactor;
        prepare( session, keyspace, replicationFactor );
    }

    public CassandraPathDB( PathMappedStorageConfig config )
    {
        this.config = config;

        String host = (String) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_HOST );
        int port = (Integer) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_PORT );
        String username = (String) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_USER );
        String password = (String) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_PASS );

        Cluster.Builder builder = Cluster.builder().withoutJMXReporting().addContactPoint( host ).withPort( port );
        if ( isNotBlank( username ) && isNotBlank( password ) )
        {
            logger.debug( "Build with credentials, user: {}, pass: ****", username );
            builder.withCredentials( username, password );
        }
        cluster = builder.build();

        logger.debug( "Connecting to Cassandra, host:{}, port:{}", host, port );
        session = cluster.connect();

        keyspace = (String) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_KEYSPACE );
        Integer replica = (Integer) config.getProperty( CassandraPathDBUtils.PROP_CASSANDRA_REPLICATION_FACTOR );
        if ( replica != null )
        {
            replicationFactor = replica;
        }
        prepare( session, keyspace, replicationFactor );
    }

    private void prepare( Session session, String keyspace, int replicationFactor )
    {
        session.execute( CassandraPathDBUtils.getSchemaCreateKeyspace( keyspace, replicationFactor ) );
        session.execute( CassandraPathDBUtils.getSchemaCreateTablePathmap( keyspace ) );
        session.execute( CassandraPathDBUtils.getSchemaCreateTableReversemap( keyspace ) );
        session.execute( CassandraPathDBUtils.getSchemaCreateTableReclaim( keyspace ) );
        session.execute( CassandraPathDBUtils.getSchemaCreateTableFileChecksum( keyspace ) );

        MappingManager manager = new MappingManager( session );

        pathMapMapper = manager.mapper( DtxPathMap.class, keyspace );
        reverseMapMapper = manager.mapper( DtxReverseMap.class, keyspace );
        reclaimMapper = manager.mapper( DtxReclaim.class, keyspace );
        fileChecksumMapper = manager.mapper( DtxFileChecksum.class, keyspace );

        preparedExistFileQuery = session.prepare( "SELECT count(*) FROM " + keyspace
                                                                  + ".pathmap WHERE filesystem=? and parentpath=? and filename=?;" );
        preparedExistFileQuery.setConsistencyLevel( QUORUM );

        preparedExistQuery = session.prepare( "SELECT filename FROM " + keyspace
                                                              + ".pathmap WHERE filesystem=? and parentpath=? and filename IN ? LIMIT 1;" );
        preparedExistQuery.setConsistencyLevel( QUORUM );

        preparedListQuery =
                        session.prepare( "SELECT * FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=?;" );

        preparedListCheckEmpty = session.prepare(
                        "SELECT count(*) FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=?;" );

        preparedContainingQuery = session.prepare( "SELECT filesystem FROM " + keyspace
                                                                   + ".pathmap WHERE filesystem IN ? and parentpath=? and filename=?;" );

        preparedReverseMapIncrement =
                        session.prepare( "UPDATE " + keyspace + ".reversemap SET paths = paths + ? WHERE fileid=?;" );
        preparedReverseMapIncrement.setConsistencyLevel( ONE );

        preparedReverseMapReduction =
                        session.prepare( "UPDATE " + keyspace + ".reversemap SET paths = paths - ? WHERE fileid=?;" );
        preparedReverseMapReduction.setConsistencyLevel( ONE );

        asyncJobExecutor = new AsyncJobExecutor( config );
    }

    @Override
    public void close()
    {
        if ( cluster != null ) // close only if the session and cluster were built by self
        {
            asyncJobExecutor.shutdownAndWaitTermination();
            session.close();
            cluster.close();
            logger.debug( "Cassandra connection closed" );
        }
    }

    public Session getSession()
    {
        return session;
    }

    @Override
    public Set<String> getFileSystemContaining( Collection<String> candidates, String path )
    {
        logger.debug( "Get fileSystem containing path {}, candidates: {}", path, candidates );
        if ( ROOT_DIR.equals( path ) )
        {
            return emptySet();
        }
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound = preparedContainingQuery.bind( candidates, parentPath, filename );
        ResultSet result = session.execute( bound );
        return result.all().stream().map( row -> row.get( 0, String.class ) ).collect( Collectors.toSet() );
    }

    /**
     * Get the first fileSystem in the candidates containing the path.
     *
     * The CQL query results are not returned in the order in which the key was specified in the IN clause.
     * The results are returned in the natural order of the column so we can not rely on the query like '...limit 1'.
     */
    @Override
    public String getFirstFileSystemContaining( List<String> candidates, String path )
    {
        logger.debug( "Get first fileSystem containing path {}, candidates: {}", path, candidates );
        Set<String> ret = getFileSystemContaining( candidates, path );
        if ( !ret.isEmpty() )
        {
            for ( String candidate : candidates )
            {
                if ( ret.contains( candidate ) )
                {
                    return candidate;
                }
            }
        }
        return null;
    }

    /**
     * List files under specified path.
     */
    @Override
    public List<PathMap> list( String fileSystem, String path, FileType fileType )
    {
        return list( fileSystem, path, false, 0, fileType );
    }

    @Override
    public List<PathMap> list( String fileSystem, String path, boolean recursive, int limit, FileType fileType )
    {
        if ( recursive )
        {
            List<PathMap> ret = new ArrayList<>();
            traverse( fileSystem, path, pathMap -> ret.add( pathMap ), limit, fileType );
            return ret;
        }
        else
        {
            String parentPath = PathMapUtils.normalizeParentPath( path );
            Result<DtxPathMap> ret = boundAndRunListQuery( fileSystem, parentPath );
            return ret.all().stream().filter( dtxPathMap -> matchFileType( dtxPathMap, fileType ) )
                          .collect( Collectors.toList() );
        }
    }

    private Result<DtxPathMap> boundAndRunListQuery( String fileSystem, String parentPath )
    {
        BoundStatement bound = preparedListQuery.bind( fileSystem, parentPath );
        ResultSet result = session.execute( bound );
        return pathMapMapper.map( result );
    }

    private final static DtxPathMap FAKE_ROOT_OBJ = new DtxPathMap(); // if path is ROOT_DIR, use a FAKE_ROOT_OBJ

    @Override
    public void traverse( String fileSystem, String path, Consumer<PathMap> consumer, int limit, FileType fileType )
    {
        logger.debug( "Traverse fileSystem: {}, path: {}", fileSystem, path );

        DtxPathMap root;
        if ( ROOT_DIR.equals( path ) )
        {
            root = FAKE_ROOT_OBJ;
        }
        else
        {
            if ( !path.endsWith( "/" ) )
            {
                path += "/";
            }
            String parentPath = PathMapUtils.getParentPath( path );
            String filename = PathMapUtils.getFilename( path );
            root = pathMapMapper.get( fileSystem, parentPath, filename );
            if ( root == null )
            {
                logger.debug( "Root not found, fileSystem: {}, parentPath: {}, filename: {}", fileSystem, parentPath, filename );
                return;
            }
        }

        TreeTraverser<PathMap> traverser = new TreeTraverser<PathMap>()
        {
            @Override
            public Iterable<PathMap> children( PathMap cur )
            {
                String parentPath;
                if ( cur == FAKE_ROOT_OBJ )
                {
                    parentPath = ROOT_DIR;
                }
                else
                {
                    parentPath = Paths.get( cur.getParentPath(), cur.getFilename() ).toString();
                }
                Result<DtxPathMap> ret = boundAndRunListQuery( fileSystem, parentPath );
                return new ArrayList<>( ret.all() );
            }
        };
        AtomicInteger count = new AtomicInteger( 0 );
        AtomicBoolean reachResultSetLimit = new AtomicBoolean( false );
        try
        {
            traverser.preOrderTraversal( root ).forEach( dtxPathMap -> {
                if ( limit > 0 && count.get() >= limit )
                {
                    reachResultSetLimit.set( true );
                    throw new RuntimeException(); // forEach does not have break
                }
                if ( dtxPathMap != root && matchFileType( dtxPathMap, fileType ) )
                {
                    consumer.accept( dtxPathMap );
                    count.incrementAndGet();
                }
            } );
        }
        catch ( RuntimeException e )
        {
            if ( reachResultSetLimit.get() )
            {
                logger.info( "Reach result set limit " + limit );
            }
            else
            {
                throw e;
            }
        }
    }

    private boolean matchFileType( PathMap dtxPathMap, FileType fileType )
    {
        String filename = dtxPathMap.getFilename();
        return fileType == null || fileType == all || fileType == dir && filename.endsWith( "/" )
                        || fileType == file && !filename.endsWith( "/" );
    }

    @Override
    public long getFileLength( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getSize();
        }
        return -1;
    }

    public DtxPathMap getPathMap( String fileSystem, String path )
    {
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        if ( parentPath == null || filename == null )
        {
            logger.debug( "getPathMap, fileSystem:{}, parentPath:{}, filename:{}", fileSystem, parentPath, filename );
            return null;
        }
        return pathMapMapper.get( fileSystem, parentPath, filename );
    }

    @Override
    public long getFileLastModified( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null && pathMap.getFileId() != null )
        {
            return pathMap.getCreation().getTime();
        }
        return -1;
    }

    /**
     * Check if the specified path exist. If the path does not end with /, e.g., "foo/bar", we need to check both "foo/bar"
     * and "foo/bar/".
     * @return FileType.{file/dir} if exist. Null if not exist.
     */
    @Override
    public FileType exists( String fileSystem, String path )
    {
        if ( ROOT_DIR.equals( path ) )
        {
            return dir;
        }

        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound;
        if ( filename.endsWith( "/" ) )
        {
            bound = preparedExistQuery.bind( fileSystem, parentPath, Arrays.asList( filename ) );
        }
        else
        {
            bound = preparedExistQuery.bind( fileSystem, parentPath, Arrays.asList( filename, filename + "/" ) );
        }
        ResultSet result = session.execute( bound );
        FileType ret = getFileTypeOrNull( result );
        if ( ret != null )
        {
            logger.trace( "{} exists in fileSystem {}, fileType: {}", path, fileSystem, ret );
        }
        else
        {
            logger.trace( "{} not exists in fileSystem {}", path, fileSystem );
        }
        return ret;
    }

    @Override
    public boolean existsFile( String fileSystem, String path )
    {
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound = preparedExistFileQuery.bind( fileSystem, parentPath, filename );
        ResultSet result = session.execute( bound );
        Row row = result.one();
        boolean exists = false;
        if ( row != null )
        {
            long count = row.get( 0, Long.class );
            exists = count > 0;
        }
        logger.trace( "File {} {} in fileSystem {}", path, ( exists ? "exists" : "not exists" ), fileSystem );
        return exists;
    }

    private FileType getFileTypeOrNull( ResultSet result )
    {
        Row row = result.one();
        if ( row != null )
        {
            String f = row.get( 0, String.class );
            FileType ret;
            if ( f.endsWith( "/" ) )
            {
                ret = dir;
            }
            else
            {
                ret = file;
            }
            return ret;
        }
        return null;
    }

    @Override
    public void insert( String fileSystem, String path, Date creation, Date expiration, String fileId, long size,
                        String fileStorage, String checksum )
    {
        DtxPathMap pathMap = new DtxPathMap();
        pathMap.setFileSystem( fileSystem );
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );
        pathMap.setParentPath( parentPath );
        pathMap.setFilename( filename );
        pathMap.setCreation( creation );
        pathMap.setExpiration( expiration );
        pathMap.setFileId( fileId );
        pathMap.setFileStorage( fileStorage );
        pathMap.setSize( size );
        pathMap.setChecksum( checksum );
        insert( pathMap );
    }

    private void insert( PathMap pathMap )
    {
        logger.debug( "Insert: {}", pathMap );

        final String fileSystem = pathMap.getFileSystem();
        final String parent = pathMap.getParentPath();

        asyncJobExecutor.execute(() -> makeDirs( fileSystem, parent ));

        String path = PathMapUtils.normalize( parent, pathMap.getFilename() );
        PathMap prev = getPathMap( fileSystem, path );
        final boolean existedPathMap = prev != null;
        if ( existedPathMap )
        {
            delete( fileSystem, path );
        }

        String checksum = pathMap.getChecksum();

        if ( isNotBlank( checksum ) )
        {
            final FileChecksum existedChecksum = fileChecksumMapper.get( checksum );

            if ( existedChecksum != null )
            {
                logger.info( "File checksum conflict, should use existed file storage" );
                final String deprecatedStorage = pathMap.getFileStorage();
                ( (DtxPathMap) pathMap ).setFileStorage( existedChecksum.getStorage() );
                ( (DtxPathMap) pathMap ).setFileId( existedChecksum.getFileId() );
                ( (DtxPathMap) pathMap ).setChecksum( existedChecksum.getChecksum() );
                // Need to mark the generated file storage path as reclaimed to remove it.
                final String deprecatedFileId = PathMapUtils.getRandomFileId();
                asyncJobExecutor.execute(() -> reclaim( deprecatedFileId, deprecatedStorage, checksum ));
            }
            else
            {
                logger.debug( "File checksum not exists, marked current file {} as primary", pathMap );
                fileChecksumMapper.save(
                        new DtxFileChecksum( checksum, pathMap.getFileId(), pathMap.getFileStorage() ) );
            }
        }

        pathMapMapper.save( (DtxPathMap) pathMap );

        // update reverse mapping
        asyncJobExecutor.execute(() -> addToReverseMap( pathMap.getFileId(), PathMapUtils.marshall( fileSystem, path ) ));

        logger.debug( "Insert finished: {}", pathMap.getFilename() );
    }

    @Override
    public boolean isDirectory( String fileSystem, String path )
    {
        if ( !path.endsWith( "/" ) )
        {
            path += "/";
        }
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound = preparedExistQuery.bind( fileSystem, parentPath, Arrays.asList( filename ) );
        ResultSet result = session.execute( bound );
        return notNull( result );
    }

    @Override
    public boolean isFile( String fileSystem, String path )
    {
        if ( path.endsWith( "/" ) )
        {
            return false;
        }
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound = preparedExistQuery.bind( fileSystem, parentPath, Arrays.asList( filename ) );
        ResultSet result = session.execute( bound );
        return notNull( result );
    }

    private boolean notNull( ResultSet result )
    {
        return result.one() != null;
    }

    @Override
    public boolean delete( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap == null )
        {
            logger.debug( "File not exists, {}", pathMap );
            return true;
        }

        String fileId = pathMap.getFileId();
        if ( fileId == null )
        {
            // can only remove empty dir
            if ( isEmptyDirectory( fileSystem, path ) )
            {
                logger.info( "Delete empty dir, {}", pathMap );
                pathMapMapper.delete( pathMap.getFileSystem(), pathMap.getParentPath(), pathMap.getFilename() );
                return true;
            }
            logger.warn( "Can not delete non-empty directory, {}", pathMap );
            return false;
        }

        logger.debug( "Delete pathMap, {}", pathMap );
        pathMapMapper.delete( pathMap.getFileSystem(), pathMap.getParentPath(), pathMap.getFilename() );

        ReverseMap reverseMap = deleteFromReverseMap( pathMap.getFileId(), PathMapUtils.marshall( fileSystem, path ) );
        if ( reverseMap == null || reverseMap.getPaths() == null || reverseMap.getPaths().isEmpty() )
        {
            // clean checksum in checksum table as no file id refer to it.
            String checksum = pathMap.getChecksum();
            if ( isNotBlank( checksum ) )
            {
                logger.debug( "Delete file checksum, {}", checksum );
                fileChecksumMapper.delete( checksum );
            }
            // reclaim, but not remove from reverse table immediately (for race-detection/double-check)
            reclaim( fileId, pathMap.getFileStorage(), checksum );
        }

        return true;
    }

    private boolean isEmptyDirectory( String fileSystem, String path )
    {
        path = PathMapUtils.normalizeParentPath( path );
        BoundStatement bound = preparedListCheckEmpty.bind( fileSystem, path );
        ResultSet result = session.execute( bound );
        Row row = result.one();
        boolean empty = false;
        if ( row != null )
        {
            long count = row.get( 0, Long.class );
            empty = count <= 0;
        }
        logger.trace( "Dir '{}' is {} in fileSystem '{}'", path, ( empty ? "empty" : "not empty" ), fileSystem );
        return empty;
    }

    private ReverseMap deleteFromReverseMap( String fileId, String path )
    {
        logger.debug( "Delete from reverseMap, fileId: {}, path: {}", fileId, path );
        BoundStatement bound = preparedReverseMapReduction.bind();
        Set<String> reduction = new HashSet();
        reduction.add( path );
        bound.setSet( 0, reduction );
        bound.setString( 1, fileId );
        session.execute( bound );
        return reverseMapMapper.get( fileId );
    }

    private void addToReverseMap( String fileId, String path )
    {
        logger.debug( "Add to reverseMap, fileId: {}, path: {}", fileId, path );
        BoundStatement bound = preparedReverseMapIncrement.bind();
        Set<String> increment = new HashSet();
        increment.add( path );
        bound.setSet( 0, increment );
        bound.setString( 1, fileId );
        session.execute( bound );
    }

    private void reclaim( String fileId, String fileStorage, String checksum )
    {
        DtxReclaim reclaim = new DtxReclaim( fileId, new Date(), fileStorage, checksum );
        logger.debug( "Reclaim, {}", reclaim );
        reclaimMapper.save( reclaim );
    }

    @Override
    public String getStorageFile( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return checkExpirationAnd( fileSystem, path, pathMap, ( t ) -> t.getFileStorage() );
        }
        return null;
    }

    private <R> R checkExpirationAnd( String fileSystem, String path, PathMap pathMap, Function<PathMap, R> function )
    {
        Date expiration = pathMap.getExpiration();
        if ( expiration != null && expiration.getTime() < System.currentTimeMillis() )
        {
            logger.debug( "File expired, fileSystem: {}, path: {}, expiration: {}", fileSystem, path, expiration );
            delete( fileSystem, path );
            return null;
        }
        return function.apply( pathMap );
    }

    @Override
    public boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        PathMap pathMap = getPathMap( fromFileSystem, fromPath );
        if ( pathMap == null )
        {
            logger.warn( "Source not found, {}:{}", fromFileSystem, fromPath );
            return false;
        }

        PathMap target = getPathMap( toFileSystem, toPath );
        if ( target != null )
        {
            logger.info( "Target already exists, delete it. {}:{}", toFileSystem, toPath );
            delete( toFileSystem, toPath );
        }

        String toParentPath = PathMapUtils.getParentPath( toPath );
        String toFilename = PathMapUtils.getFilename( toPath );
        target = new DtxPathMap( toFileSystem, toParentPath, toFilename, pathMap.getFileId(), pathMap.getCreation(),
                                 pathMap.getExpiration(), pathMap.getSize(), pathMap.getFileStorage(),
                                 pathMap.getChecksum() );
        insert( target );
        return true;
    }

    @Override
    public void makeDirs( String fileSystem, String path )
    {
        logger.debug( "Make dir, fileSystem: {}, path: {}", fileSystem, path );

        if ( ROOT_DIR.equals( path ) )
        {
            return;
        }
        if ( !path.endsWith( "/" ) )
        {
            path += "/";
        }

        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );

        BoundStatement bound = preparedExistQuery.bind( fileSystem, parentPath, Arrays.asList( filename ) );
        ResultSet result = session.execute( bound );
        if ( notNull( result ) )
        {
            logger.debug( "Dir already exists, fileSystem: {}, path: {}", fileSystem, path );
            return;
        }

        DtxPathMap pathMap = new DtxPathMap();
        pathMap.setFileSystem( fileSystem );
        pathMap.setParentPath( parentPath );
        pathMap.setFilename( filename );

        final List<DtxPathMap> parents = PathMapUtils.getParentsBottomUp( pathMap, ( fSystem, pPath, fName ) -> {
            DtxPathMap p = new DtxPathMap();
            p.setFileSystem( fSystem );
            p.setParentPath( pPath );
            p.setFilename( fName );
            return p;
        } );

        List<DtxPathMap> persist = new ArrayList<>();
        persist.add( pathMap );
        persist.addAll( parents );

        logger.debug( "Persist: {}", persist );
        persist.forEach( e -> pathMapMapper.save( e ) );
    }

    @Override
    public List<Reclaim> listOrphanedFiles( int limit )
    {
        // timestamp data type is encoded as the number of milliseconds since epoch
        Date cur = new Date();
        long threshold = getReclaimThreshold( cur, config.getGCGracePeriodInHours() );
        logger.debug( "listOrphanedFiles, cur: {}, threshold: {}, limit: {}", cur, new Date( threshold ), limit );
        ResultSet result;
        String baseQuery = "SELECT * FROM " + keyspace + ".reclaim WHERE partition = 0 AND deletion < ?";
        if ( limit > 0 )
        {
            result = session.execute( baseQuery + " limit ?;", threshold, limit );
        }
        else
        {
            result = session.execute( baseQuery + ";", threshold );
        }
        Result<DtxReclaim> ret = reclaimMapper.map( result );
        return new ArrayList<>( ret.all() );
    }

    @Override
    public void removeFromReclaim( Reclaim reclaim )
    {
        reclaimMapper.delete( (DtxReclaim) reclaim );
    }

    private long getReclaimThreshold( Date date, int gcGracePeriodInHours )
    {
        long ret = date.getTime();
        if ( gcGracePeriodInHours <= 0 )
        {
            return ret;
        }
        return ret - Duration.ofHours( gcGracePeriodInHours ).toMillis();
    }

}
