package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.test.elasticfacets.utils.AbstractNodesTests;

public abstract class AbstractFacetTest extends AbstractNodesTests {

	protected int numberOfRuns() {
		return 5;
	}

   protected void logFacet(TermsFacet facet) {
		for (int facet_pos=0;facet_pos<facet.getEntries().size();facet_pos++) {
			
			logger.debug("Evaluating pos={}: term={} count={}", facet_pos,
					facet.getEntries().get(facet_pos).getTerm(),facet.getEntries().get(facet_pos).getCount());
		}

	}

}
