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
package org.commonjava.storage.pathmapped.pathdb.jpa;

import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaPathKey;
import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaPathMap;
import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaReclaim;
import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaReverseKey;
import org.commonjava.storage.pathmapped.pathdb.jpa.model.JpaReverseMap;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.model.ReverseMap;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.util.PathMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class JPAPathDB
                implements PathDB
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final EntityManagerFactory factory;

    private final EntityManager entitymanager;

    public JPAPathDB( String persistenceUnitName )
    {
        factory = Persistence.createEntityManagerFactory( persistenceUnitName );
        entitymanager = factory.createEntityManager();
    }

    public List<PathMap> list( String fileSystem, String path, FileType fileType )
    {
        if ( path.endsWith( "/" ) )
        {
            path = path.substring( 0, path.length() - 1 );
        }

        Query query = entitymanager.createQuery(
                        "Select p from JpaPathMap p where p.pathKey.fileSystem=?1 and p.pathKey.parentPath=?2" )
                                   .setParameter( 1, fileSystem )
                                   .setParameter( 2, path );

        List<PathMap> list = query.getResultList();
        return list;
    }

    @Override
    public List<PathMap> list( String fileSystem, String path, boolean recursive, int limit, FileType fileType )
    {
        return list( fileSystem, path, fileType );
    }

    @Override
    public PathMap getPathMap(String fileSystem, String path)
    {
        return findPathMap( fileSystem, path );
    }

    @Override
    public long getFileLength( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getSize();
        }
        return -1;
    }

    private JpaPathMap findPathMap( String fileSystem, String path )
    {
        return entitymanager.find( JpaPathMap.class, getPathKey( fileSystem, path ) );
    }

    @Override
    public long getFileLastModified( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getCreation().getTime();
        }
        return -1;
    }

    @Override
    public FileType exists( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            if ( pathMap.getFileId() != null )
            {
                return FileType.file;
            }
            return FileType.dir;
        }
        return null;
    }

    @Override
    public boolean existsFile( String fileSystem, String path )
    {
        return exists( fileSystem, path ) != null;
    }

    @Override
    public void insert( String fileSystem, String path, Date creation, Date expiration, String fileId, long size, String fileStorage, String checksum )
    {
        JpaPathMap pathMap = new JpaPathMap();
        JpaPathKey pathKey = getPathKey( fileSystem, path );
        pathMap.setPathKey( pathKey );
        pathMap.setCreation( creation );
        pathMap.setExpiration( expiration );
        pathMap.setFileId( fileId );
        pathMap.setFileStorage( fileStorage );
        pathMap.setSize( size );
        insert( pathMap, checksum );
    }

    private void insert( PathMap pathMap, String checksum )
    {
        logger.debug( "Insert: {}", pathMap );

        String fileSystem = pathMap.getFileSystem();
        String parent = pathMap.getParentPath();

        makeDirs( fileSystem, parent );

        String path = PathMapUtils.normalize( parent, pathMap.getFilename() );

        // before insertion, we need to get the prev entry and check for reclaim
        JpaPathKey key = ( (JpaPathMap) pathMap ).getPathKey();
        PathMap prev = entitymanager.find( JpaPathMap.class, key );
        if ( prev != null )
        {
            delete( fileSystem, path );
        }

        // insert path mapping and reverse mapping
        transactionAnd( () -> {
            entitymanager.persist( pathMap );
        } );

        // insert reverse mapping
        addToReverseMap( pathMap.getFileId(), fileSystem, path );
    }

    private void addToReverseMap( String fileId, String fileSystem, String path )
    {
        HashSet<String> updatedPaths = new HashSet<>();
        ReverseMap reverseMap = getReverseMap( fileId );
        if ( reverseMap != null )
        {
            updatedPaths.addAll( reverseMap.getPaths() );
        }
        updatedPaths.add( PathMapUtils.marshall( fileSystem, path ) );
        ReverseMap updatedReverseMap = new JpaReverseMap( new JpaReverseKey( fileId, 0 ), updatedPaths );
        transactionAnd( () -> entitymanager.persist( updatedReverseMap ) );
    }

    private void transactionAnd( Runnable job )
    {
        entitymanager.getTransaction().begin();
        job.run();
        entitymanager.getTransaction().commit();
    }

    @Override
    public boolean isDirectory( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getFileId() == null;
        }
        return false;
    }

    @Override
    public boolean isFile( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getFileId() != null;
        }
        return false;
    }

    @Override
    public boolean delete( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap == null )
        {
            logger.debug( "File not exists, {}", pathMap );
            return true;
        }

        String fileId = pathMap.getFileId();
        if ( fileId == null )
        {
            logger.debug( "Can not delete a directory, {}", pathMap );
            return false;
        }

        transactionAnd( () -> entitymanager.remove( pathMap ) );

        removeFromReverseMap( fileSystem, path, pathMap );
        return true;
    }

    @Override
    public boolean delete(String fileSystem, String path, boolean force)
    {
        return delete( fileSystem, path );
    }

    private void removeFromReverseMap( String fileSystem, String path, PathMap pathMap )
    {
        String fileId = pathMap.getFileId();
        ReverseMap reverseMap = getReverseMap( fileId );
        if ( reverseMap != null )
        {
            HashSet<String> updatedPaths = new HashSet<>( reverseMap.getPaths() );
            updatedPaths.remove( PathMapUtils.marshall( fileSystem, path ) );

            if ( updatedPaths.isEmpty() )
            {
                // reclaim, but not remove from reverse table immediately (for race-detection/double-check)
                reclaim( fileId, pathMap.getFileStorage() );
            }
            else
            {
                ReverseMap updatedReverseMap = new JpaReverseMap( new JpaReverseKey( fileId, 0 ), updatedPaths );
                transactionAnd( () -> entitymanager.persist( updatedReverseMap ) );
            }
        }
        else
        {
            reclaim( fileId, pathMap.getFileStorage() );
        }
    }

    @Override
    public String getStorageFile( String fileSystem, String path )
    {
        PathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getFileStorage();
        }
        return null;
    }

    @Override
    public boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        PathMap pathMap = findPathMap( fromFileSystem, fromPath );
        if ( pathMap == null )
        {
            logger.warn( "Source PathKey not found, {}:{}", fromFileSystem, fromPath );
            return false;
        }

        JpaPathKey to = getPathKey( toFileSystem, toPath );
        PathMap target = findPathMap( toFileSystem, toPath );
        if ( target != null )
        {
            logger.info( "Target PathKey already exists, delete it. {}", to );
            delete( toFileSystem, toPath );
        }

        // check parent paths
        String parentPath = to.getParentPath();
        String toParentPath = to.getParentPath();
        if ( !parentPath.equals( toParentPath ) )
        {
            makeDirs( toFileSystem, toParentPath );
        }

        //TODO: need to implement checksum de-dupe in future, and add checksum here
        transactionAnd( () -> {
            entitymanager.persist( new JpaPathMap( to, pathMap.getFileId(), pathMap.getCreation(), pathMap.getExpiration(), pathMap.getSize(),
                                                   pathMap.getFileStorage(), "" ) );
        } );
        return true;
    }

    @Override
    public boolean copy(String fromFileSystem, String fromPath, String toFileSystem, String toPath, Date creation, Date expiration)
    {
        return copy( fromFileSystem, fromPath, toFileSystem, toPath );
    }

    @Override
    public void expire(String fileSystem, String path, Date expiration) {
    }

    @Override
    public void makeDirs( String fileSystem, String path )
    {
        logger.debug( "Make dir, fileSystem: {}, path: {}", fileSystem, path );

        if ( PathMapUtils.ROOT_DIR.equals( path ) )
        {
            return;
        }
        if ( !path.endsWith( "/" ) )
        {
            path += "/";
        }

        JpaPathMap pathMap = findPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            logger.debug( "Dir exists, {}:{}", fileSystem, path );
            return;
        }

        JpaPathKey pathKey = getPathKey( fileSystem, path );
        pathMap = new JpaPathMap();
        pathMap.setPathKey( pathKey );

        final List<JpaPathMap> parents = PathMapUtils.getParentsBottomUp( pathMap, ( fSystem, pPath, fName ) -> {
            JpaPathMap p = new JpaPathMap();
            p.setPathKey( new JpaPathKey( fSystem, pPath, fName ) );
            return p;
        } );

        List<JpaPathMap> persist = new ArrayList<>();
        persist.add( pathMap );

        logger.debug( "Get persist: {}", persist );

        for ( JpaPathMap p : parents )
        {
            JpaPathMap o = entitymanager.find( JpaPathMap.class, p.getPathKey() );
            if ( o != null )
            {
                break;
            }
            persist.add( p );
        }

        transactionAnd( () -> persist.forEach( p -> entitymanager.persist( p ) ) );
    }

    private void reclaim( String fileId, String fileStorage )
    {
        transactionAnd( () -> {
            entitymanager.persist( new JpaReclaim( fileId, new Date(), fileStorage ) );
        } );
    }

    private ReverseMap getReverseMap( String fileId )
    {
        return entitymanager.find( JpaReverseMap.class, new JpaReverseKey( fileId, 0 ) );
    }

    @Override
    public List<Reclaim> listOrphanedFiles( int limit )
    {
        Query query = entitymanager.createQuery( "Select r from Reclaim r" );
        return query.getResultList();
    }

    @Override
    public void removeFromReclaim( Reclaim reclaim )
    {
        entitymanager.remove( reclaim );
    }

    @Override
    public Set<String> getFileSystemContaining( Collection<String> candidates, String path )
    {
        return Collections.emptySet();
    }

    @Override
    public String getFirstFileSystemContaining( List<String> candidates, String path )
    {
        return null;
    }

    @Override
    public void traverse( String fileSystem, String path, Consumer<PathMap> consumer, int limit, FileType fileType )
    {
    }

    private JpaPathKey getPathKey( String fileSystem, String path )
    {
        String parentPath = PathMapUtils.getParentPath( path );
        String filename = PathMapUtils.getFilename( path );
        return new JpaPathKey( fileSystem, parentPath, filename );
    }

}
