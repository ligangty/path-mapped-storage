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

import org.commonjava.storage.pathmapped.model.Reclaim;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;
import java.util.Objects;

@Entity
@Table( name = "reclaim" )
public class JpaReclaim implements Reclaim
{
    @Id
    @Column( name = "fileid" )
    private String fileId;

    @Column
    private Date deletion;

    @Column
    private String storage;

    public JpaReclaim()
    {
    }

    public JpaReclaim( String fileId, Date deletion, String storage )
    {
        this.fileId = fileId;
        this.deletion = deletion;
        this.storage = storage;
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
    public Date getDeletion()
    {
        return deletion;
    }

    public void setDeletion( Date deletion )
    {
        this.deletion = deletion;
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
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        JpaReclaim reclaim = (JpaReclaim) o;
        return fileId.equals( reclaim.fileId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileId );
    }
}
