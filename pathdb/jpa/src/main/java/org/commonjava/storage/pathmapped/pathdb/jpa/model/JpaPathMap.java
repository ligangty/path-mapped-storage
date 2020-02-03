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

import org.commonjava.storage.pathmapped.model.PathMap;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Date;
import java.util.Objects;

@Entity
@Table( name = "pathmap" )
public class JpaPathMap implements PathMap
{
    @EmbeddedId
    private JpaPathKey pathKey;

    @Column( name = "fileid" )
    private String fileId;

    @Column
    private Date creation;

    @Column
    private Date expiration;

    @Column
    private long size;

    @Column( name = "filestorage" )
    private String fileStorage;

    @Column( name = "checksum" )
    private String checksum;

    public JpaPathMap()
    {
    }

    public JpaPathMap( JpaPathKey pathKey, String fileId, Date creation, Date expiration, long size, String fileStorage, String checksum )
    {
        this.pathKey = pathKey;
        this.fileId = fileId;
        this.creation = creation;
        this.expiration = expiration;
        this.size = size;
        this.fileStorage = fileStorage;
        this.checksum = checksum;
    }

    public JpaPathKey getPathKey()
    {
        return pathKey;
    }

    public void setPathKey( JpaPathKey pathKey )
    {
        this.pathKey = pathKey;
    }

    @Override
    public String getFileSystem()
    {
        return pathKey.getFileSystem();
    }

    @Override
    public String getParentPath()
    {
        return pathKey.getParentPath();
    }

    @Override
    public String getFilename()
    {
        return pathKey.getFilename();
    }

    @Override
    public String getFileId()
    {
        return fileId;
    }

    public void setFileId( String fileId )
    {
        this.fileId = fileId;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    public void setSize( long size )
    {
        this.size = size;
    }

    @Override
    public String getFileStorage()
    {
        return fileStorage;
    }

    public void setFileStorage( String fileStorage )
    {
        this.fileStorage = fileStorage;
    }

    @Override
    public Date getCreation()
    {
        return creation;
    }

    public void setCreation( Date creation )
    {
        this.creation = creation;
    }

    @Override
    public Date getExpiration()
    {
        return expiration;
    }

    public void setExpiration( Date expiration )
    {
        this.expiration = expiration;
    }

    @Override
    public String getChecksum()
    {
        return checksum;
    }

    public void setChecksum( String checksum )
    {
        this.checksum = checksum;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        JpaPathMap pathMap = (JpaPathMap) o;
        return pathKey.equals( pathMap.pathKey );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pathKey );
    }

    @Override
    public String toString()
    {
        return "JpaPathMap{" + "pathKey=" + pathKey + ", fileId='" + fileId + '\'' + ", creation=" + creation
                        + ", expiration=" + expiration + ", size=" + size + ", fileStorage='" + fileStorage + '\''
                        + ", checksum='" + checksum + '\'' + '}';
    }
}
