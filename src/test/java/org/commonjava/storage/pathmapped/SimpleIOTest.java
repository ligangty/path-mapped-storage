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

import ch.qos.logback.core.util.FileSize;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.commonjava.storage.pathmapped.core.FileInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class SimpleIOTest
        extends AbstractCassandraFMTest
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final String root = "/root";

    private final String parent = "parent";

    private final String pathParent = root + "/" + parent;

    private final String sub1 = "sub1";

    private final String sub2 = "sub2";

    private final String pathSub1 = pathParent + "/" + sub1;

    private final String pathSub2 = pathParent + "/" + sub2;

    private final String file1 = "target1.txt";

    private final String file2 = "target2.txt";

    private final String path1 = pathSub1 + "/" + file1;

    private final String path2 = pathSub2 + "/" + file2;

    private final String simpleContent = "This is a test";

    @Test
    public void readWrittenFile()
            throws Exception
    {
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            assertNotNull( os );
            IOUtils.write( simpleContent.getBytes(), os );
        }

        try (InputStream is = fileManager.openInputStream( TEST_FS, path1 ))
        {
            assertNotNull( is );
            String result = new String( IOUtils.toByteArray( is ), Charset.defaultCharset() );
            assertThat( result, equalTo( simpleContent ) );
        }
    }

    @Test
    public void getRealFile()
            throws Exception
    {
        readWrittenFile();
        String realFilePath = fileManager.getFileStoragePath( TEST_FS, path1 );
        assertThat( realFilePath, notNullValue() );
        assertThat( FileUtils.readFileToString( Paths.get( getBaseDir(), realFilePath ).toFile() ),
                    equalTo( simpleContent ) );
    }

    @Test
    public void getFileMetadata()
            throws Exception
    {
        assertThat( fileManager.getFileLength( TEST_FS, null ), equalTo( 0 ) );
        assertThat( fileManager.getFileLastModified( TEST_FS, null ), equalTo( -1L ) );
        readWrittenFile();
        assertThat( fileManager.getFileLength( TEST_FS, path1 ), equalTo( simpleContent.length() ) );
        assertThat( fileManager.getFileLastModified( TEST_FS, path1 ) > 0, equalTo( true ) );
    }

    @Test
    public void read1MbFile()
            throws Exception
    {
        assertReadFileOfSize( "1mb" );
    }

    @Test
    public void read11MbFile()
            throws Exception
    {
        assertReadFileOfSize( "11mb" );
    }

    private void assertReadFileOfSize( String s )
            throws IOException
    {
        int sz = (int) FileSize.valueOf( s ).getSize();
        byte[] src = new byte[sz];

        Random rand = new Random();
        rand.nextBytes( src );

        try (OutputStream out = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            IOUtils.write( src, out );
        }

        try (InputStream stream = fileManager.openInputStream( TEST_FS, path1 ))
        {
            byte[] result = IOUtils.toByteArray( stream );
            assertThat( result, equalTo( src ) );
        }
    }

    @Test
    public void writeToFileWithLines()
            throws Exception
    {

        writeWithCount( fileManager.openOutputStream( TEST_FS, path1 ) );

        final File file = new File( path1 );
        try (InputStream is = fileManager.openInputStream( TEST_FS, path1 ))
        {
            final List<String> lines = IOUtils.readLines( is );
            assertThat( lines.size(), equalTo( COUNT ) );
        }

    }

    @Test
    public void overwriteFile()
            throws Exception
    {
        try (OutputStream stream = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            String longer = "This is a really really really long string";
            stream.write( longer.getBytes() );
        }

        String shorter = "This is a short string";
        try (OutputStream stream = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            stream.write( shorter.getBytes() );
        }

        long fileLength = fileManager.getFileLength( TEST_FS, path1 );
        assertThat( fileLength, equalTo( (long) shorter.getBytes().length ) );

        try (InputStream stream = fileManager.openInputStream( TEST_FS, path1 ))
        {
            String content = IOUtils.toString( stream );
            assertThat( content, equalTo( shorter ) );
        }

    }

    @Test
    public void fileRepeatedRead()
            throws Exception
    {
        writeWithContent( fileManager.openOutputStream( TEST_FS, path1 ), simpleContent );
        InputStream s1 = null;
        InputStream s2 = null;

        try
        {

            s1 = fileManager.openInputStream( TEST_FS, path1 );
            s2 = fileManager.openInputStream( TEST_FS, path1 );

            logger.info( "READ first " );
            String out1 = IOUtils.toString( s1 );
            logger.info( "READ second " );
            String out2 = IOUtils.toString( s2 );

            assertThat( "first reader returned wrong data", out1, equalTo( simpleContent ) );
            assertThat( "second reader returned wrong data", out2, equalTo( simpleContent ) );
        }
        finally
        {
            logger.info( "CLOSE first thread" );
            IOUtils.closeQuietly( s1 );
            logger.info( "CLOSE second thread" );
            IOUtils.closeQuietly( s2 );
        }

    }

    @Test
    public void delete()
            throws IOException
    {
        try (InputStream is = fileManager.openInputStream( TEST_FS, path1 ))
        {
            assertThat( is, nullValue() );
        }
        writeWithContent( fileManager.openOutputStream( TEST_FS, path1 ), simpleContent );
        try (InputStream is = fileManager.openInputStream( TEST_FS, path1 ))
        {
            assertThat( is, notNullValue() );
        }
        assertThat( fileManager.delete( TEST_FS, path1 ), equalTo( true ) );
        try (InputStream is = fileManager.openInputStream( TEST_FS, path1 ))
        {
            assertThat( is, nullValue() );
        }
        //NOTE: not allow to delete a folder
        assertThat( fileManager.delete( TEST_FS, pathSub1 + "/" ), equalTo( false ) );
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, pathSub1, true );
    }

    @Test
    public void existsFileOrDir()
            throws IOException
    {
        assertThat( fileManager.exists( TEST_FS, null ), equalTo( false ) );
        final String tempRoot = "/temp-exists";
        final String tempPathParent = tempRoot + "/" + parent;
        final String tempPath = tempPathParent + "/" + file1;
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, tempPath, false );
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, tempPathParent, false );
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, tempRoot, false );
        writeWithContent( fileManager.openOutputStream( TEST_FS, tempPath ), simpleContent );
        assertThat( fileManager.exists( TEST_FS, tempPath ), equalTo( true ) );
        assertThat( fileManager.exists( TEST_FS, tempPath + "/" ), equalTo( false ) );
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, tempPathParent, true );
        assertPathWithChecker( ( f, p ) -> fileManager.exists( f, p ), TEST_FS, tempRoot, true );
    }

    @Test
    public void isFileOrDir()
            throws IOException
    {
        assertThat( fileManager.isFile( TEST_FS, null ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, null ), equalTo( false ) );
        final String tempRoot = "/temp-dir";
        final String tempPathParent = tempRoot + "/" + parent;
        final String tempPath = tempPathParent + "/" + file1;
        assertThat( fileManager.isFile( TEST_FS, path1 ), equalTo( false ) );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPath, false );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathParent, false );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempRoot, false );
        writeWithContent( fileManager.openOutputStream( TEST_FS, tempPath ), simpleContent );
        assertThat( fileManager.isFile( TEST_FS, tempPath ), equalTo( true ) );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPath, false );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathParent, true );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempRoot, true );
    }

    @Test
    public void makeDirs()
    {
        final String tempRoot = "/temp-dirs";
        final String tempPathParent = tempRoot + "/" + parent;
        final String tempPathSub = tempPathParent + "/" + sub1;
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempRoot, false );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathParent, false );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathSub, false );
        fileManager.makeDirs( TEST_FS, tempPathSub );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempRoot, true );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathParent, true );
        assertPathWithChecker( ( f, p ) -> fileManager.isDirectory( f, p ), TEST_FS, tempPathSub, true );
    }

    @Test
    public void listEntriesInSameFolder()
            throws IOException
    {
        List<String> lists = Arrays.asList( fileManager.list( TEST_FS, null ) );
        assertThat( lists.isEmpty(), equalTo( true ) );
        final String file3 = "target3.txt";
        final String path3 = pathSub1 + "/" + file3;
        writeWithContent( fileManager.openOutputStream( TEST_FS, path1 ), simpleContent );
        writeWithContent( fileManager.openOutputStream( TEST_FS, path3 ), simpleContent );
        lists = Arrays.asList( fileManager.list( TEST_FS, pathSub1 ) );
        assertThat( lists, hasItems( file1, file3 ) );
        lists = Arrays.asList( fileManager.list( TEST_FS, pathParent ) );
        assertThat( lists, hasItems( sub1 + "/" ) );
    }

    @Test
    public void listEntriesInDiffFolders()
            throws IOException
    {
        writeWithContent( fileManager.openOutputStream( TEST_FS, path1 ), simpleContent );
        writeWithContent( fileManager.openOutputStream( TEST_FS, path2 ), simpleContent );
        List<String> lists = Arrays.asList( fileManager.list( TEST_FS, pathParent ) );
        assertThat( lists, hasItems( sub1 + "/", sub2 + "/" ) );

        lists = Arrays.asList( fileManager.list( TEST_FS, pathSub1 ) );
        assertThat( lists, hasItems( file1 ) );
        lists = Arrays.asList( fileManager.list( TEST_FS, pathSub2 ) );
        assertThat( lists, hasItems( file2 ) );
    }

    @Test
    public void listHugeNumOfEntries()
            throws IOException
    {
        int numOfFiles = 500;
        String[] files = new String[numOfFiles];
        for ( int i = 0; i < numOfFiles; i++ )
        {
            files[i] = "file" + i + ".txt";
            String filePath = pathSub1 + "/" + files[i];
            writeWithContent( fileManager.openOutputStream( TEST_FS, filePath ), simpleContent );
        }
        long start = System.currentTimeMillis();
        List<String> lists = Arrays.asList( fileManager.list( TEST_FS, pathSub1 ) );
        assertThat( lists, hasItems( files ) );
        long end = System.currentTimeMillis();
        logger.info( "Listing {} files took {} milliseconds", numOfFiles, end - start );
    }

    @Test
    public void simpleCopy()
            throws IOException
    {
        final String TEMP_FS = "TEMP_FS";
        assertThat( fileManager.exists( TEST_FS, path1 ), equalTo( false ) );
        assertThat( fileManager.exists( TEMP_FS, path2 ), equalTo( false ) );
        writeWithContent( fileManager.openOutputStream( TEST_FS, path1 ), simpleContent );
        fileManager.copy( TEST_FS, path1, TEMP_FS, path2 );
        assertThat( fileManager.exists( TEMP_FS, path2 ), equalTo( true ) );
        try (InputStream is = fileManager.openInputStream( TEMP_FS, path2 ))
        {
            assertNotNull( is );
            String result = new String( IOUtils.toByteArray( is ), Charset.defaultCharset() );
            assertThat( result, equalTo( simpleContent ) );
        }
    }

    @Test
    public void gc()
            throws Exception
    {
        readWrittenFile();
        File realFile = Paths.get( getBaseDir(), fileManager.getFileStoragePath( TEST_FS, path1 ) ).toFile();
        assertThat( realFile.exists(), equalTo( true ) );
        assertThat( FileUtils.readFileToString( realFile ), equalTo( simpleContent ) );
        fileManager.delete( TEST_FS, path1 );
        fileManager.gc();
        assertThat( realFile.exists(), equalTo( false ) );
    }

    private void writeWithCount( OutputStream stream )
    {
        try (OutputStream os = stream)
        {
            for ( int i = 0; i < COUNT; i++ )
            {
                os.write( String.format( "%d\n", i ).getBytes() );
            }

        }
        catch ( final IOException e )
        {
            e.printStackTrace();
        }
    }

    private void writeWithContent( OutputStream stream, String content )
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

    @FunctionalInterface
    private interface PathChecker<T>
    {
        T checkPath( String fileSystem, String path );
    }

    private void assertPathWithChecker( PathChecker<Boolean> checker, String fileSystem, String path, boolean expected )
    {
        assertThat( checker.checkPath( fileSystem, path ), equalTo( expected ) );
        assertThat( checker.checkPath( fileSystem, path + "/" ), equalTo( expected ) );
    }

    @Override
    protected void clearData()
    {
        super.clearData();
        fileManager.delete( TEST_FS, path1 );
        fileManager.delete( TEST_FS, path2 );
        for ( Map.Entry<FileInfo, Boolean> entry : fileManager.gc().entrySet() )
        {
            logger.info( "{}", entry.getKey() );
        }
    }
}
