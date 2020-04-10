package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpGeoEnrichment extends Enrichment<String> {
    public static final String UNKNOWN_HOST_ERROR_MESSAGE = "Unknown host.";
    static final String GEOCODE_FEATURE = "geo";
    static final String CITY_FIELD_ENDING = "city";
    static final String COUNTRY_FIELD_ENDING = "country";
    static final String STATE_FIELD_ENDING = "state";
    static final String LATITUDE_FIELD_ENDING = "latitude";
    static final String LONGITUDE_FIELD_ENDING = "longitude";
    private static final Logger LOG = LoggerFactory.getLogger(IpGeoEnrichment.class);
    private final DatabaseReader cityDatabase;

    private IpGeoEnrichment(DatabaseReader cityDatabase) {
        super(GEOCODE_FEATURE, String.class);
        this.cityDatabase = cityDatabase;
    }

    public static IpGeoEnrichment create(EnrichmentReferenceFiles referenceFiles) throws IOException {
        return new IpGeoEnrichment(referenceFiles.getGeoCityDatabase());
    }

    @Override
    public void addEnrichment(LogEvent event, String fieldName) {
        String ipField = getEnrichmentFieldValue(event, fieldName);

        try {
            InetAddress ipAddress = InetAddress.getByName(ipField);
            cityDatabase.tryCity(ipAddress).ifPresent(response -> addFields(event, fieldName, response));
        } catch (UnknownHostException e) {
            event.reportError(fieldName, ipField, featureName, UNKNOWN_HOST_ERROR_MESSAGE);
            LOG.error(String.format("Unknown host for ip '%s' in field '%s'", ipField, fieldName), e);
        } catch (GeoIp2Exception e) {
            event.reportError(fieldName, ipField, featureName, "GeoIp exception.");
            LOG.error(String.format("Geocoding exception for ip '%s' in field '%s'", ipField, fieldName), e);
        } catch (IOException e) {
            event.reportError(fieldName, ipField, featureName, "IO exception.");
            LOG.error(String.format("IOException while geocoding ip '%s' in field '%s'", ipField, fieldName), e);
        }
    }

    private void addFields(LogEvent event, String fieldName, CityResponse cityResponse) {
        City city = cityResponse.getCity();
        if (city != null) {
            addFieldValue(event, fieldName, CITY_FIELD_ENDING, city.getName());
        }

        Country country = cityResponse.getCountry();
        if (country != null) {
            addFieldValue(event, fieldName, COUNTRY_FIELD_ENDING, country.getIsoCode());
        }

        Subdivision subdivision = cityResponse.getMostSpecificSubdivision();
        if (subdivision != null) {
            addFieldValue(event, fieldName, STATE_FIELD_ENDING, subdivision.getName());
        }

        Location location = cityResponse.getLocation();
        if (location != null) {
            addFieldValue(event, fieldName, LATITUDE_FIELD_ENDING, location.getLatitude());
            addFieldValue(event, fieldName, LONGITUDE_FIELD_ENDING, location.getLongitude());
        }
    }

}
