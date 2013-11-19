package org.leskes.elasticfacets;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.hppc.LongLongOpenHashMap;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A date histogram facet collector that adds a separate internal facet on each bucket.
 */
public class FacetedDateHistogramCollector extends FacetExecutor {

    private final TimeZoneRounding tzRounding;
    private final IndexNumericFieldData indexFieldData;

    final Recycler.V<LongLongOpenHashMap> counts;
    final Recycler.V<LongObjectOpenHashMap<FacetedDateHistogramFacet.Entry>> entries;

	private final FacetExecutor internalExampleCollector;

	private static ESLogger logger = Loggers.getLogger(FacetedDateHistogramCollector.class);
    private InternalCollectorFactory colFactory;

    public FacetedDateHistogramCollector(
            IndexNumericFieldData indexFieldData,
			TimeZoneRounding tzRounding,
        	FacetParser internalParser,
			byte[] internalFacetConfig,
            SearchContext searchContext) throws IOException {
        super();
        this.indexFieldData = indexFieldData;
        this.tzRounding = tzRounding;

        CacheRecycler cacheRecycler = searchContext.cacheRecycler();

        this.counts = cacheRecycler.longLongMap(-1);
        this.entries = cacheRecycler.longObjectMap(-1);

        colFactory = new InternalCollectorFactory(internalParser, internalFacetConfig, searchContext);

        logger.debug("Faceted date histogram: Test-running internal facet processor ");
		this.internalExampleCollector = colFactory.createInternalCollector();
	}

    @Override
    public FacetExecutor.Collector collector() {
        return new Collector();
    }

    protected static class InternalCollectorFactory {
		private FacetParser internalParser;
		private byte[] internalFacetConfig;
        private SearchContext searchContext;

        public InternalCollectorFactory(FacetParser internalParser, byte[] internalFacetConfig, SearchContext searchContext) {
            this.internalParser = internalParser;
			this.internalFacetConfig = internalFacetConfig;
            this.searchContext = searchContext;
        }
		
		public FacetExecutor createInternalCollector() throws IOException {
			XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(internalFacetConfig);
            XContentParser.Token token = parser.nextToken(); // advance past opening brace
            if (token != XContentParser.Token.START_OBJECT)
                throw new FacetPhaseExecutionException("faceted_date_histogram", "Internal facet definition is malformed");
	        try {
	            return internalParser.parse("facet", parser, searchContext);
	        } finally {
	            parser.close();
	        }
		}

	}

    class Collector extends FacetExecutor.Collector {

        private LongValues values;
        private final FacetedDateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new FacetedDateHistogramProc(counts.v(), tzRounding, colFactory,
                    (LongObjectOpenHashMap<FacetedDateHistogramFacet.Entry>) entries.v());
        }

        @Override
        public void postCollection() {
            final Object[] localEntries = entries.v().values;
            for (Object o: localEntries) {
                if (o == null) continue;
                ((FacetedDateHistogramFacet.Entry) o).collector().postCollection();
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getLongValues();
            histoProc.setNextReader(context);
        }
    }

	@Override
	public InternalFacet buildFacet(String facetName) {
        // Java throws classcastexception here when accessing entries.values directly, some kind of generics problem
        final Object[] localEntries = entries.v().values;
        for (Object o: localEntries){
			if (o == null) continue;
			((FacetedDateHistogramFacet.Entry)o).facetize(facetName);
		}
		return new FacetedDateHistogramFacet(facetName, entries);
	}
	
	public static class FacetedDateHistogramProc extends LongFacetAggregatorBase {

		protected final TimeZoneRounding tzRounding;

		final protected  InternalCollectorFactory collectorFactory;
		
        private AtomicReaderContext currentContext = null;
        private LongObjectOpenHashMap<FacetedDateHistogramFacet.Entry> entries;

        public FacetedDateHistogramProc(LongLongOpenHashMap v,
                                        TimeZoneRounding tzRounding,
                                        InternalCollectorFactory collectorFactor,
                                        LongObjectOpenHashMap<FacetedDateHistogramFacet.Entry> entries
                                        )
		{
			this.tzRounding = tzRounding;
			this.collectorFactory = collectorFactor;
            this.entries = entries;
        }
		
		public void setNextReader(AtomicReaderContext context) throws IOException {
            // Java throws classcastexception here when accessing entries.values directly, some kind of generics problem
			final Object[] localEntries = entries.values;
            for (Object o: localEntries) {
				if (o == null) continue;

                FacetedDateHistogramFacet.Entry e = (FacetedDateHistogramFacet.Entry)o;
				
				e.collector().setNextReader(context);
			}
            currentContext = context;
		}

		public void onValue(int docId, long value) {
			long bucketTime = tzRounding.calc(value);
			
			FacetedDateHistogramFacet.Entry entry;
			entry = getOrCreateEntry(value, bucketTime);
			
			try {
				entry.collector().collect(docId);
			} catch (Exception e) {
				throw new RuntimeException("Error running an internal collector",e);
			}			

		}

		private FacetedDateHistogramFacet.Entry getOrCreateEntry(
				long value, long time) {
			FacetedDateHistogramFacet.Entry entry;
			entry = entries.get(time);
			if (entry == null) {
				try {
					entry = new FacetedDateHistogramFacet.Entry(time, collectorFactory.createInternalCollector());
					entry.collector().setNextReader(currentContext);
				} catch (Exception e) {
					throw new RuntimeException("Error creating an internal collector",e);
				}
				
				entries.put(time, entry);
			}
			return entry;
		}
	
		
	



	}
}