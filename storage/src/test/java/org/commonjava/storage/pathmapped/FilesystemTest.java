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
import org.commonjava.storage.pathmapped.model.Filesystem;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.OutputStream;

import static org.junit.Assert.*;

public class FilesystemTest
        extends AbstractCassandraFMTest
{
    @Test
    public void duplicateFileTest()
            throws Exception
    {
        byte[] bytes = simpleContent.getBytes();
        long fileSize = bytes.length;

        try (OutputStream os = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            IOUtils.write( bytes, os );
        }
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, path2 ))
        {
            IOUtils.write( bytes, os );
        }

        // filesystem's size equals to just one file's size
        Filesystem filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( fileSize ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 2L ) );

        // after deleting one file, filesystem size not change, file count reduced by one
        fileManager.delete( TEST_FS, path1 );
        filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( fileSize ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 1L ) );

        fileManager.delete( TEST_FS, path2 );
        filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( 0L ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 0L ) );
    }

    @Test
    public void runTest()
            throws Exception
    {
        byte[] bytes = simpleContent.getBytes();
        long fileSize = bytes.length;

        final String anotherContent = "This is another test";
        byte[] bytes2 = anotherContent.getBytes();
        long fileSize2 = bytes2.length;

        try (OutputStream os = fileManager.openOutputStream( TEST_FS, path1 ))
        {
            IOUtils.write( bytes, os );
        }
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, path2 ))
        {
            IOUtils.write( bytes2, os );
        }

        // filesystem's size equals to file A + B
        Filesystem filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( fileSize + fileSize2 ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 2L ) );

        // after deleting file A, filesystem size equals to file B
        fileManager.delete( TEST_FS, path1 );
        filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( fileSize2 ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 1L ) );

        fileManager.delete( TEST_FS, path2 );
        filesystem = fileManager.getFilesystem( TEST_FS );
        assertThat( filesystem.getSize(), CoreMatchers.equalTo( 0L ) );
        assertThat( filesystem.getFileCount(), CoreMatchers.equalTo( 0L ) );
    }
}
