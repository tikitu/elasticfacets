package org.leskes.elasticfacets;

import org.apache.lucene.search.Collector;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.hppc.LongObjectMap;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;
import org.elasticsearch.common.hppc.ObjectContainer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class FacetedDateHistogramFacet extends InternalFacet {

    public static final String TYPE = "faceted_date_histogram";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("facetedDateHistogram"));

    protected static final Comparator<EntryBase> comparator = new Comparator<EntryBase>() {

        public int compare(EntryBase o1, EntryBase o2) {
            // push nulls to the end
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                }
                return 1;
            }
            if (o2 == null) {
                return -1;
            }
            return (o1.time < o2.time ? -1 : (o1.time == o2.time ? 0 : 1));
        }
    };


    public static class EntryBase {
    	public final long time;
    	
    	public EntryBase(long time) {
    		this.time = time;
    	}
    	
    }

    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public static class Entry extends EntryBase {
        protected InternalFacet internalFacet;
        protected FacetExecutor executor;
        protected FacetExecutor.Collector collector;

        public Entry(long time, FacetExecutor executor) {
        	super(time);
            this.executor = executor;
        }
        public Entry(long time) {
        	this(time,null);

        }
        
        public void facetize() {
        	this.internalFacet = executor.buildFacet("facet"); // internal entry is called "facet": that's the json key it should appear under
        	this.executor = null;
        }
        
        public Facet facet() {
        	return internalFacet;
        }

        /**
         * Unlike FacetExecutor.collector() this guarantees to always return <i>the same</i> collector.
         * @return
         */
        public FacetExecutor.Collector collector() {
            if (executor == null) return null;
            if (collector == null) {
                collector = executor.collector();
            }
            return collector;
        }

    }
    
    /**
     * Entry which can contain multiple facts per entry. used for reducing
     */
    public static class MultiEntry extends EntryBase {
    	public List<InternalFacet> facets;
    	
    	public MultiEntry(long time) {
        	super(time);
    		this.facets = new ArrayList<InternalFacet>();
    	}
    }


    protected LongObjectOpenHashMap<Entry> entries;
    
    protected List<Entry> entriesAsList;



    public List<Entry> collapseToAList() {
        if (entriesAsList == null) {
            entriesAsList = Lists.newArrayListWithCapacity(entries.size());
            final Object[] values = entries.values;
            for (int i = 0; i < values.length; i++) {
                Entry value  = (Entry) values[i];
                if (value != null) entriesAsList.add(value);
            }
        }
        return entriesAsList;
    }

    private FacetedDateHistogramFacet() {
    }

    private FacetedDateHistogramFacet(String facetName) {
        super(facetName);
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    public FacetedDateHistogramFacet(String name, LongObjectOpenHashMap<Entry> entries) {
        super(name);
        this.entries = entries;
    }

    @Override()
    public Facet reduce(ReduceContext context) {
        List<Facet> facets = context.facets();
        if (facets.size() == 1) {
            // we need to sort it
            FacetedDateHistogramFacet internalFacet = (FacetedDateHistogramFacet) facets.get(0);
            List<Entry> entries = internalFacet.collapseToAList();
            Collections.sort(entries, comparator);
            return internalFacet;
        }

        Recycler.V<LongObjectOpenHashMap<MultiEntry>> map = context.cacheRecycler().longObjectMap(-1);

        for (Facet facet : facets) {
        	FacetedDateHistogramFacet histoFacet = (FacetedDateHistogramFacet) facet;
            for (Entry entry : histoFacet.collapseToAList()) {
            	MultiEntry current = map.v().get(entry.time);
                if (current == null) {
                	current = new MultiEntry(entry.time);
                    map.v().put(current.time, current);
                }
                current.facets.add((InternalFacet) entry.internalFacet);
            }
        }

        // sort
        Object[] values = map.v().values;
        Arrays.sort(values, (Comparator) comparator);
        List<MultiEntry> ordered = new ArrayList<MultiEntry>(map.v().size());
        for (int i = 0; i < map.v().size(); i++) {
        	MultiEntry value = (MultiEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }
        map.release();

        // just initialize it as already ordered facet
        FacetedDateHistogramFacet ret = new FacetedDateHistogramFacet(getName());
        ret.entriesAsList = new ArrayList<Entry>(ordered.size());
        
        for (MultiEntry me : ordered) {
        	Entry e = new Entry(me.time);
        	InternalFacet f = me.facets.get(0);
            List<Facet> facetsAsFacets = new ArrayList<Facet>(me.facets); // cannot pass List<InternalFacet> to ReduceContext, deep sighs
            e.internalFacet = (InternalFacet) f.reduce(new ReduceContext(context.cacheRecycler(), facetsAsFacets));
        	ret.entriesAsList.add(e);
        }

        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : collapseToAList()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time);
            entry.internalFacet.toXContent(builder,params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static FacetedDateHistogramFacet readFacetedHistogramFacet(StreamInput in) throws IOException {
    	FacetedDateHistogramFacet facet = new FacetedDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        entries = new LongObjectOpenHashMap<Entry>(size);
        for (int i = 0; i < size; i++) {
        	Entry e = new Entry(in.readLong(),null);
        	
        	BytesReference internal_type = in.readBytesReference();
            e.internalFacet = (InternalFacet) Streams.stream(internal_type).readFacet(in);
            entries.put(e.time, e);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(entries.size());
        for (Entry e : collapseToAList()) {
            out.writeLong(e.time);
            out.writeBytesReference(e.internalFacet.streamType());
            e.internalFacet.writeTo(out);
        }
    }


	public String getType() {
        return TYPE;
    }

    static InternalFacet.Stream STREAM = new InternalFacet.Stream() {
        public Facet readFacet(StreamInput in) throws IOException {
            return readFacetedHistogramFacet(in);
        }
    };

    public static void registerStreams() {
        InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);
    }

}