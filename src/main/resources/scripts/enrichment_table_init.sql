create schema if not exists cyberreference;
use cyberreference;
create table enrichment(event_field_value VARCHAR NOT NULL, reference_data_set VARCHAR NOT NULL, original_source_reference VARCHAR, original_source VARCHAR, validStartTime DATE, validEndTime DATE, CONSTRAINT pk PRIMARY KEY (event_field_value, reference_data_set));
create table enrichment_source(original_source_id VARCHAR, updateTime DATE, update_successful BOOLEAN, constraint pk PRIMARY KEY (original_source_id));