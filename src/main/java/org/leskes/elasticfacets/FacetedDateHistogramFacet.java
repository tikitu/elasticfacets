package org.leskes.elasticfacets;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
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

        public Entry(long time, FacetExecutor collector) {
        	super(time);
            this.executor = collector;
        }
        public Entry(long time) {
        	this(time,null);

        }
        
        public void facetize(String facetName) {
        	this.internalFacet = executor.buildFacet(facetName);
        	this.executor = null;
        }
        
        public Facet facet() {
        	return internalFacet;
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


    private String name;
    

    protected Recycler.V<ExtTLongObjectHashMap<Entry>> entries;
    
    protected List<Entry> entriesAsList;



    public List<Entry> collapseToAList() {
        if (entriesAsList == null) {
        	entriesAsList = new ArrayList<Entry>(entries.v().valueCollection());
            releaseEntries();
        }
        return entriesAsList;
    }
    

    private FacetedDateHistogramFacet() {
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    public FacetedDateHistogramFacet(String name, Recycler.V<ExtTLongObjectHashMap<Entry>> entries) {
    	// Now we own the entries map. It is MUST come from the cache recycler..
        this.name = name;
        
        this.entries = entries;
    }

    void releaseEntries() {
        entries.release();
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

        Recycler.V<ExtTLongObjectHashMap<MultiEntry>> map = context.cacheRecycler().longObjectMap(-1);

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
        Object[] values = map.v().internalValues();
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
        FacetedDateHistogramFacet ret = new FacetedDateHistogramFacet();
        ret.name = name;
        ret.entriesAsList = new ArrayList<Entry>(ordered.size());
        
        for (MultiEntry me : ordered) {
        	Entry e = new Entry(me.time);
        	InternalFacet f = me.facets.get(0);
            e.internalFacet = (InternalFacet) f.reduce(new ReduceContext(context.cacheRecycler(), (List<Facet>) me.facets));
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
        builder.startObject(name);
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
        name = in.readString();

        int size = in.readVInt();
        entries = SearchContext.current().cacheRecycler().longObjectMap(-1);
        for (int i = 0; i < size; i++) {
        	Entry e = new Entry(in.readLong(),null);
        	
        	BytesReference internal_type = in.readBytesReference();
            e.internalFacet = (InternalFacet) Streams.stream(internal_type).readFacet(in);
            entries.v().put(e.time, e);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(entries.v().size());
        for (Entry e : collapseToAList()) {
            out.writeLong(e.time);
            out.writeBytesReference(e.internalFacet.streamType());
            e.internalFacet.writeTo(out);
        }
        releaseEntries();
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