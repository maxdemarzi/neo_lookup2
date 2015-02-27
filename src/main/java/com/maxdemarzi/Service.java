package com.maxdemarzi;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@Path("/v1")
public class Service {

    private static GraphDatabaseService db;

    public Service(@Context GraphDatabaseService graphDatabaseService) {
        db = graphDatabaseService;
    }

    static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LoadingCache<Integer, Long> segments = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .build(
                    new CacheLoader<Integer, Long>() {
                        public Long load(Integer segmentId) {
                            return getSegmentNodeId(segmentId);
                        }
                    });

    private static Long getSegmentNodeId(Integer segmentId){
        final Node node = db.findNode(Labels.Segment, "segmentId", segmentId);
        return node.getId();
    }

    @GET
    @Path("/migrate")
    public String migrate(@Context GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            schema.constraintFor(Labels.Segment)
                    .assertPropertyIsUnique("segmentId")
                    .create();
            tx.success();
        }
        // Wait for indexes to come online
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            schema.awaitIndexesOnline(1, TimeUnit.DAYS);
            tx.success();
        }
        return "Migrated!";
    }

    @POST
    @Path("/lookup")
    public String Lookup(String body, @Context GraphDatabaseService db) throws IOException {
        ArrayList<Long> idsFound = new ArrayList<>();
        HashMap<String, List<Integer>> input;
        input = objectMapper.readValue(body, HashMap.class);
        List<Integer> segmentIds = input.get("segmentIds");

        try (Transaction tx = db.beginTx()) {
            for (int segmentId : segmentIds) {
                final Node node = db.findNode(Labels.Segment, "segmentId", segmentId);

                if (node != null) {
                    idsFound.add(node.getId());
                }
            }
            tx.success();
        }

        return idsFound.toString();
    }

    @POST
    @Path("/cachedlookup")
    public String CachedLookup(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {
        ArrayList<Long> idsFound = new ArrayList<>();
        HashMap<String, List<Integer>> input;
        input = objectMapper.readValue(body, HashMap.class);
        List<Integer> segmentIds = input.get("segmentIds");

        try (Transaction tx = db.beginTx()) {
            for (int segmentId : segmentIds) {
                final Node node = db.getNodeById(segments.get(segmentId));

                if (node != null) {
                    idsFound.add(node.getId());
                }
            }
            tx.success();
        }

        return idsFound.toString();
    }
}
