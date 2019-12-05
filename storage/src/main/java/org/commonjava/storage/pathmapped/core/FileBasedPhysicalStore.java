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
package org.commonjava.storage.pathmapped.core;

import org.commonjava.storage.pathmapped.spi.FileInfo;
import org.commonjava.storage.pathmapped.spi.PhysicalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.commonjava.storage.pathmapped.util.PathMapUtils.getRandomFileId;

public class FileBasedPhysicalStore implements PhysicalStore
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final File baseDir;

    public FileBasedPhysicalStore( File baseDir )
    {
        this.baseDir = baseDir;
    }

    @Override
    public FileInfo getFileInfo( String fileSystem, String path )
    {
        String id = getRandomFileId();
        String dir = getStorageDir( id );
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFileId( id );
        fileInfo.setFileStorage( Paths.get( dir, id ).toString() );
        return fileInfo;
    }

    private static final int LEVEL_1_DIR_LENGTH = 2;

    private static final int LEVEL_2_DIR_LENGTH = 2;

    private static final int DIR_LENGTH = LEVEL_1_DIR_LENGTH + LEVEL_2_DIR_LENGTH;

    private String getStorageDir( String fileId )
    {
        String folder = fileId.substring( 0, LEVEL_1_DIR_LENGTH );
        String subFolder = fileId.substring( LEVEL_1_DIR_LENGTH, DIR_LENGTH );
        return folder + "/" + subFolder;
    }

    @Override
    public OutputStream getOutputStream( FileInfo fileInfo ) throws IOException
    {
        File f = new File( baseDir, fileInfo.getFileStorage() );
        File dir = f.getParentFile();
        if ( !dir.isDirectory() )
        {
            dir.mkdirs();
        }
        return new FileOutputStream( f );
    }

    @Override
    public InputStream getInputStream( String storageFile ) throws IOException
    {
        File f = new File( baseDir, storageFile );
        if ( f.isDirectory() || !f.exists() )
        {
            logger.debug( "Target file not exists, file: {}", f.getAbsolutePath() );
            return null;
        }
        return new FileInputStream( f );
    }

    @Override
    public boolean delete( FileInfo fileInfo )
    {
        File f = new File( baseDir, fileInfo.getFileStorage() );
        try
        {
            Files.deleteIfExists( Paths.get( f.getAbsolutePath() ) );
        }
        catch ( IOException e )
        {
            logger.error( "Failed to delete file: " + fileInfo, e );
            return false;
        }
        return true;
    }

}
