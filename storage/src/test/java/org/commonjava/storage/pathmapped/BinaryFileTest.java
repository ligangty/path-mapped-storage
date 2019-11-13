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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class BinaryFileTest
        extends AbstractCassandraFMTest
{
    private void writeBinaryFile( OutputStream os, ByteArrayOutputStream written )
            throws IOException
    {
        try (OutputStream cos = os)
        {
            for ( int i = 0; i < 255; i++ )
            {
                cos.write( i );
                written.write( i );
            }
        }
    }

    @Test
    public void shouldReadBinaryFile()
            throws IOException
    {
        List<String> failures = new ArrayList<>();
        final String fName = "binary-file.bin";
        File binaryFile = temp.newFile( fName );
        OutputStream os = fileManager.openOutputStream( TEST_FS, binaryFile.getPath() );
        ByteArrayOutputStream written = new ByteArrayOutputStream();
        writeBinaryFile( os, written );

        InputStream actual = fileManager.openInputStream( TEST_FS, binaryFile.getPath() );
        int pos = 0;
        int exp, act;

        ByteArrayInputStream expected = new ByteArrayInputStream( written.toByteArray() );
        while ( ( exp = expected.read() ) != -1 )
        {
            act = actual.read();
            pos++;

            if ( act != exp )
            {
                failures.add( String.format( "Failure at position %d. Expected %d, got %d", pos, exp, act ) );
            }
        }

        if ( !failures.isEmpty() )
        {
            Assert.fail( "Failures: " + failures );
        }
    }
}
