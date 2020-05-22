package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class IpGeoEnrichment implements EnrichmentReferenceDataSource {
    public static final String UNKNOWN_HOST_ERROR_MESSAGE = "Unknown host.";
    public static final String GEOCODE_FEATURE = "geo";
    static final String CITY_FIELD_ENDING = "city";
    static final String COUNTRY_FIELD_ENDING = "country";
    static final String STATE_FIELD_ENDING = "state";
    static final String LATITUDE_FIELD_ENDING = "latitude";
    static final String LONGITUDE_FIELD_ENDING = "longitude";
    private static final Logger LOG = LoggerFactory.getLogger(IpGeoEnrichment.class);
    private final DatabaseReader cityDatabase;

    private IpGeoEnrichment(DatabaseReader cityDatabase) {
        this.cityDatabase = cityDatabase;
    }

    public static IpGeoEnrichment create(EnrichmentReferenceFiles referenceFiles) throws IOException {
        return new IpGeoEnrichment(referenceFiles.getGeoCityDatabase());
    }

    private void addFields(Map<String, Object> geoEnrichmentFields, CityResponse cityResponse) {
        City city = cityResponse.getCity();
        if (city != null && StringUtils.isNotBlank(city.getName())) {
            geoEnrichmentFields.put(CITY_FIELD_ENDING, city.getName());
        }

        Country country = cityResponse.getCountry();
        if (country != null && StringUtils.isNotBlank(country.getIsoCode())) {
            geoEnrichmentFields.put(COUNTRY_FIELD_ENDING, country.getIsoCode());
        }

        Subdivision subdivision = cityResponse.getMostSpecificSubdivision();
        if (subdivision != null && StringUtils.isNotBlank(subdivision.getName())) {
            geoEnrichmentFields.put(STATE_FIELD_ENDING, subdivision.getName());
        }

        Location location = cityResponse.getLocation();
        if (location != null) {
            if (location.getLatitude() != null) {
                geoEnrichmentFields.put(LATITUDE_FIELD_ENDING, location.getLatitude());
            }
            if (location.getLongitude() != null) {
                geoEnrichmentFields.put(LONGITUDE_FIELD_ENDING, location.getLongitude());
            }
        }
    }

    @Override
    public Map<String, Object> lookup(String enrichmentReferenceData, Object ipFieldValueObject) throws Exception {

        Map<String, Object> geoEnrichments = new HashMap<>();
        String ipFieldValue = (String) ipFieldValueObject;
        try {
            if (InetAddressValidator.getInstance().isValidInet4Address(ipFieldValue)) {
                InetAddress ipAddress = InetAddress.getByName(ipFieldValue);
                cityDatabase.tryCity(ipAddress).ifPresent(response -> addFields(geoEnrichments, response));
            }
            return geoEnrichments;
        } catch (UnknownHostException e) {
            LOG.error(String.format("Unknown host for ip '%s'.", ipFieldValue), e);
            throw new Exception(UNKNOWN_HOST_ERROR_MESSAGE, e);
        } catch (GeoIp2Exception e) {
            LOG.error(String.format("Geocoding exception for ip '%s'", ipFieldValue), e);
            throw new Exception(String.format("GeoIp exception for ip '%s'.", ipFieldValue), e);
        } catch (IOException e) {
            LOG.error(String.format("IOException while geocoding ip '%s'", ipFieldValue), e);
            throw new Exception(String.format("IOException exception for ip '%s'.", ipFieldValue), e);
        }
    }
}
