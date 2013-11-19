package org.leskes.test.elasticfacets.facets;

import com.jayway.jsonassert.JsonAssert;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.elasticfacets.FacetedDateHistogramFacet;
import org.leskes.elasticfacets.FacetedDateHistogramFacet.Entry;
import org.testng.annotations.Test;

import java.util.List;

import static com.jayway.jsonassert.JsonAssert.collectionWithSize;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FacetedDateHistogramTest extends AbstractFacetTest {
   protected long documentCount =0;


   @Override
	protected void loadData() throws Exception {

		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "week1").field("date","2012-07-03T10:00:00.000Z")
								.endObject()).execute().actionGet();
		client.admin().indices().prepareFlush().setFull(true).execute()
				.actionGet();
		
		documentCount++;

		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "week2").field("date","2012-07-10T10:00:00.000Z")
								.endObject()).execute().actionGet();

		client.admin().indices().prepareRefresh().execute().actionGet();
		documentCount++;

	}
	
	@Test
	public void SimpleWeekIntervalTest() throws Exception{
		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							("{ \"my_facet\": { \"faceted_date_histogram\" : " +
									"{ \"field\": \"date\", \"size\": 2 ,\"interval\": \"week\", "+
						     "            \"facet\": { \"terms\" : { \"field\": \"tag\"}}  "+
							 "}      }      }"
								).getBytes("UTF-8"))
                    .setOperationThreading(SearchOperationThreading.NO_THREADS)
					.execute().actionGet();

			assertThat(searchResponse.getHits().totalHits(), equalTo(documentCount));

			FacetedDateHistogramFacet facet = searchResponse.getFacets().facet("my_facet");
			assertThat(facet.getName(), equalTo("my_facet"));
			List<Entry> entries = facet.collapseToAList();
			assertThat(entries.size(), equalTo(2));
			assertThat(entries.get(0).time,equalTo(1341187200000L));
			assertThat(((TermsFacet)entries.get(0).facet()).getEntries().get(0).getTerm().string(), equalTo("week1"));
			assertThat(entries.get(1).time,equalTo(1341792000000L));
			assertThat(((TermsFacet)entries.get(1).facet()).getEntries().get(0).getTerm().string(), equalTo("week2"));

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            searchResponse.toXContent(jsonBuilder, null);
            jsonBuilder.endObject();
            String json = jsonBuilder.string();
            JsonAssert.with(json)
                    .assertThat(".facets.my_facet.entries[0].facet._type", equalTo("terms"))
                    // NB:               ^^^^^^^^     !=     ^^^^^
            ;
        }
	}

}
