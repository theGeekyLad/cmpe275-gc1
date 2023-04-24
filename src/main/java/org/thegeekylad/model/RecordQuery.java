package org.thegeekylad.model;

import org.thegeekylad.util.constants.QueryType;

public class RecordQuery {
    public String id;
    public QueryType type;
    public int countResponses;

    public RecordQuery(String id, QueryType type, int countResponses) {
        this.id = id;
        this.type = type;
        this.countResponses = countResponses;
    }
}
