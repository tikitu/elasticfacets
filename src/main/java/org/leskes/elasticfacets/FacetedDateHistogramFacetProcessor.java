package org.leskes.elasticfacets;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContentGenerator;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.*;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FacetedDateHistogramFacetProcessor extends AbstractComponent implements FacetParser {

    private static final String STREAM_TYPE = "facetedDateHistogram";

    public String streamType() {
        return STREAM_TYPE;
    }

    private final ImmutableMap<String, DateFieldParser> dateFieldParsers;
	private FacetParsers processors;
	
	private static JsonFactory jsonFactory = new JsonFactory();


    @Inject
    public FacetedDateHistogramFacetProcessor(Settings settings) {
        super(settings);
        FacetedDateHistogramFacet.registerStreams();

        dateFieldParsers = MapBuilder.<String, DateFieldParser>newMapBuilder()
                .put("year", new DateFieldParser.YearOfCentury())
                .put("1y", new DateFieldParser.YearOfCentury())
                .put("quarter", new DateFieldParser.Quarter())
                .put("month", new DateFieldParser.MonthOfYear())
                .put("1m", new DateFieldParser.MonthOfYear())
                .put("week", new DateFieldParser.WeekOfWeekyear())
                .put("1w", new DateFieldParser.WeekOfWeekyear())
                .put("day", new DateFieldParser.DayOfMonth())
                .put("1d", new DateFieldParser.DayOfMonth())
                .put("hour", new DateFieldParser.HourOfDay())
                .put("1h", new DateFieldParser.HourOfDay())
                .put("minute", new DateFieldParser.MinuteOfHour())
                .put("1m", new DateFieldParser.MinuteOfHour())
                .put("second", new DateFieldParser.SecondOfMinute())
                .put("1s", new DateFieldParser.SecondOfMinute())
                .immutableMap();
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Inject
    public void SetProcessors(Set<FacetParser> processors) {
    	this.processors= new FacetParsers(processors);
    }

    public String[] types() {
        return new String[]{FacetedDateHistogramFacet.TYPE};
    }

    public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        Map<String, Object> params = null;
        String interval = null;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        boolean preZoneAdjustLargeInterval = false;
        long preOffset = 0;
        long postOffset = 0;
        float factor = 1.0f;
        Chronology chronology = ISOChronology.getInstanceUTC();

        FacetParser internalProcessor = null;
        byte[] internalConfig = null;
        
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                } else if ("facet".equals(fieldName)) {
                	token = parser.nextToken();
                	if (token != XContentParser.Token.FIELD_NAME)
                		throw new FacetPhaseExecutionException(facetName, "No facet type defined under facet node");
                	String facetType = parser.currentName();
                	internalProcessor = processors.processor(facetType);
                    if (internalProcessor == null) {
                        throw new FacetPhaseExecutionException(facetName, "No facet type found for [" + facetType + "]");
                    }
                    // Store underlying facet configuration
                	token = parser.nextToken(); // move the start of object...
                    ByteArrayOutputStream memstream = new ByteArrayOutputStream(20);
                    XContentGenerator generator = new JsonXContentGenerator(jsonFactory.createGenerator(memstream));
                    XContentHelper.copyCurrentStructure(generator, parser);
                    generator.close();
                    memstream.close();
                    internalConfig = memstream.toByteArray(); // now we're at the end of the underlying config.
                                    		
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT); // eat everything else under the "facet" clause
                	
                }
                
            } 
            else if (token.isValue()) {
                if ("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("key_field".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("interval".equals(fieldName)) {
                    interval = parser.text();
                } else if ("time_zone".equals(fieldName) || "timeZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone".equals(fieldName) || "preZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone_adjust_large_interval".equals(fieldName) || "preZoneAdjustLargeInterval".equals(fieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if ("post_zone".equals(fieldName) || "postZone".equals(fieldName)) {
                    postZone = parseZone(parser, token);
                } else if ("pre_offset".equals(fieldName) || "preOffset".equals(fieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if ("post_offset".equals(fieldName) || "postOffset".equals(fieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if ("factor".equals(fieldName)) {
                    factor = parser.floatValue();
                }
            }
        }

        if (interval == null) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        FieldMapper keyMapper = context.smartNameFieldMapper(keyField);
        if (keyMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] not found");
        }
        IndexNumericFieldData keyIndexFieldData = context.fieldData().getForField(keyMapper);

        if (internalProcessor == null) {
            throw new FacetPhaseExecutionException(facetName, "faceted histogram misses an internal facet definition.");
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateFieldParser fieldParser = dateFieldParsers.get(interval);
        if (fieldParser != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(fieldParser.parse(chronology));
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .factor(factor)
                .build();

        return new FacetedDateHistogramCollector(keyIndexFieldData, tzRounding, internalProcessor, internalConfig, context);
       
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        return TimeValue.parseTimeValue(offset, null).millis();
    }

    private DateTimeZone parseZone(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            String text = parser.text();
            int index = text.indexOf(':');
            if (index != -1) {
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(0, index)),
                        Integer.parseInt(text.substring(index + 1))
                );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

    static interface DateFieldParser {

        DateTimeField parse(Chronology chronology);

        static class WeekOfWeekyear implements DateFieldParser {

        	public DateTimeField parse(Chronology chronology) {
                return chronology.weekOfWeekyear();
            }
        }

        static class YearOfCentury implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class MonthOfYear implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }
        
        static class Quarter implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return Joda.QuarterOfYear.getField(chronology);
            }
        }

        static class HourOfDay implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateFieldParser {
            public DateTimeField parse(Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }
    }


}
