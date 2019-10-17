package org.commonjava.storage.pathmapped.model;

import java.util.Date;

public interface Reclaim
{
    String getFileId();

    Date getDeletion();

    String getStorage();
}
