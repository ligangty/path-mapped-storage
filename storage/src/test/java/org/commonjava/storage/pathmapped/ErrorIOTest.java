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
import org.apache.commons.lang.reflect.FieldUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static junit.framework.TestCase.fail;

public class ErrorIOTest
                extends AbstractCassandraFMTest
{
    private final String TEST_FS = "test";

    private final String errPath = "io/err.out";

    private final String nullPath = "io/null.out";

    @Test
    public void writeFileError() throws Exception
    {
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, errPath ))
        {
            Assert.assertNotNull( os );
            IOUtils.write( simpleContent.getBytes(), os );
            FieldUtils.writeField( os, "error", new IOException(), true );
        }

        try
        {
            try (InputStream is = fileManager.openInputStream( TEST_FS, errPath ))
            {
            }
            fail();
        }
        catch ( IOException ex )
        {
            //java.io.IOException: Could not open input stream to for path test - io/err.out: path-mapped physical file does not exist.
            System.out.println( ex );
        }
    }

    @Test
    public void writeNothing() throws Exception
    {
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, nullPath ))
        {
        }

        try (InputStream is = fileManager.openInputStream( TEST_FS, nullPath ))
        {
        }
    }

    @Override
    protected void clearData()
    {
        super.clearData();
        fileManager.delete( TEST_FS, errPath );
    }
}
