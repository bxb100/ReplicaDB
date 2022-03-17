package org.replicadb.rowset;

import java.sql.SQLException;

import com.sun.rowset.CachedRowSetImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OrcCachedRowSetImpl extends CachedRowSetImpl {
    private static final Logger LOG = LogManager.getLogger(OrcCachedRowSetImpl.class);

    public OrcCachedRowSetImpl() throws SQLException {
    }

    // TODO

}
