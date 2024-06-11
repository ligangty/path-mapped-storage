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
package org.commonjava.storage.pathmapped.pathdb.jpa.model;

import org.commonjava.storage.pathmapped.model.FileChecksum;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Objects;

@Entity
@Table( name = "filechecksum" )
public class JpaFileChecksum
        implements FileChecksum
{
    @Id
    private String checksum;

    @Column( name = "fileid" )
    private String fileId;

    @Column( name = "storage" )
    private String storage;

    public JpaFileChecksum()
    {
    }

    public JpaFileChecksum( String checksum, String fileId, String storage )
    {
        this.checksum = checksum;
        this.fileId = fileId;
        this.storage = storage;
    }

    public String getFileId()
    {
        return fileId;
    }

    public void setFileId( String fileId )
    {
        this.fileId = fileId;
    }

    public String getChecksum()
    {
        return checksum;
    }

    public void setChecksum( String checksum )
    {
        this.checksum = checksum;
    }

    @Override
    public String getStorage()
    {
        return storage;
    }

    public void setStorage( String storage )
    {
        this.storage = storage;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        JpaFileChecksum that = (JpaFileChecksum) o;
        return checksum.equals( that.checksum );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( checksum );
    }
}
