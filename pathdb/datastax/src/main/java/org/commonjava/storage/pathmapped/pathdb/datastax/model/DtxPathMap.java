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
package org.commonjava.storage.pathmapped.pathdb.datastax.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.commonjava.storage.pathmapped.model.PathMap;

import java.util.Date;
import java.util.Objects;

@Table( name = "pathmap", readConsistency = "QUORUM", writeConsistency = "ALL" )
public class DtxPathMap implements PathMap
{
    @PartitionKey(0)
    private String fileSystem;

    @PartitionKey(1)
    private String parentPath;

    @ClusteringColumn
    private String filename;

    @Column
    private String fileId;

    @Column
    private Date creation;

    @Column
    private Date expiration;

    @Column
    private long size;

    @Column
    private String fileStorage;

    @Column
    private String checksum;

    public DtxPathMap()
    {
    }

    public DtxPathMap( String fileSystem, String parentPath, String filename, String fileId, Date creation,
                       Date expiration, long size, String fileStorage, String checksum )
    {
        this.fileSystem = fileSystem;
        this.parentPath = parentPath;
        this.filename = filename;
        this.fileId = fileId;
        this.creation = creation;
        this.expiration = expiration;
        this.size = size;
        this.fileStorage = fileStorage;
        this.checksum = checksum;
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
    public String getFileSystem()
    {
        return fileSystem;
    }

    public void setFileSystem( String fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public String getParentPath()
    {
        return parentPath;
    }

    public void setParentPath( String parentPath )
    {
        this.parentPath = parentPath;
    }

    @Override
    public String getFilename()
    {
        return filename;
    }

    public void setFilename( String filename )
    {
        this.filename = filename;
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
        DtxPathMap that = (DtxPathMap) o;
        return fileSystem.equals( that.fileSystem ) && parentPath.equals( that.parentPath ) && filename.equals(
                        that.filename );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileSystem, parentPath, filename );
    }

    @Override
    public String toString()
    {
        return "DtxPathMap{" + "fileSystem='" + fileSystem + '\'' + ", parentPath='" + parentPath + '\''
                        + ", filename='" + filename + '\'' + ", fileId='" + fileId + '\'' + ", creation=" + creation
                        + ", expiration=" + expiration + ", size=" + size + ", fileStorage='" + fileStorage + '\''
                        + ", checksum='" + checksum + '\'' + '}';
    }
}
