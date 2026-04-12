-- Populate core station dimensions from staging.stops (loaded from data/stations.csv).
-- Run this script after 03_load_core_from_staging.sql so that core.dim_stop already
-- contains all stop_ids seen in the delay data.
--
-- Assumptions stated explicitly:
--   • stop_name, stop_lat, stop_lon are identical for all quays sharing a parent_station.
--     MAX() is used as the aggregate; any disagreement in the source would be silent.
--   • zone_id and station_code may vary or be absent per quay; MAX() picks a non-null
--     value when one exists, NULL otherwise.
--   • monomodal_stop_id is identical for all quays sharing a monomodal_code.
--   • The 390 Q-type stop_ids not present in stations.csv and all 39 BP-type stop_ids
--     will have monomodal_code = NULL in dim_stop. This is intentional.


-- Step 1: physical stations.
-- Derived by collapsing all quays that share the same parent_station URI.
INSERT INTO core.dim_station (station_id, stop_name, stop_lat, stop_lon, zone_id, station_code)
SELECT
    CAST(regexp_replace(parent_station, '^IDFM:', '') AS INTEGER) AS station_id,
    MAX(stop_name)    AS stop_name,
    MAX(stop_lat)     AS stop_lat,
    MAX(stop_lon)     AS stop_lon,
    MAX(zone_id)      AS zone_id,
    MAX(station_code) AS station_code
FROM staging.stops
GROUP BY parent_station
ON CONFLICT (station_id) DO NOTHING;


-- Step 2: monomodal stops.
-- One row per monomodal_code. DISTINCT ON is used because monomodal_stop_id and
-- parent_station are identical across all quays for a given monomodal_code.
INSERT INTO core.dim_monomodal_stop (monomodal_code, monomodal_stop_id, station_id)
SELECT DISTINCT ON (monomodal_code)
    monomodal_code,
    monomodal_stop_id,
    CAST(regexp_replace(parent_station, '^IDFM:', '') AS INTEGER) AS station_id
FROM staging.stops
ORDER BY monomodal_code
ON CONFLICT (monomodal_code) DO NOTHING;


-- Step 3a: classify each stop by URI type and extract the embedded numeric key.
-- Rows already enriched (stop_type IS NOT NULL) are skipped for idempotency.
UPDATE core.dim_stop
SET
    stop_type = CASE
        WHEN stop_id LIKE 'STIF:StopPoint:Q:%'  THEN 'Q'
        WHEN stop_id LIKE 'STIF:StopArea:SP:%'  THEN 'SP'
        WHEN stop_id LIKE 'STIF:StopPoint:BP:%' THEN 'BP'
        ELSE NULL
    END,
    quay_code = CASE
        WHEN stop_id LIKE 'STIF:StopPoint:Q:%'
        THEN CAST(substring(stop_id FROM 'STIF:StopPoint:Q:(\d+):') AS INTEGER)
        ELSE NULL
    END
WHERE stop_type IS NULL;


-- Step 3b: resolve monomodal_code for Q-type stops via the quay → stations mapping.
-- Only matches rows present in staging.stops; unmatched Q-ids stay NULL.
UPDATE core.dim_stop ds
SET monomodal_code = s.monomodal_code
FROM staging.stops s
WHERE ds.stop_type      = 'Q'
  AND ds.quay_code      = s.quay_code
  AND ds.monomodal_code IS NULL;


-- Step 3c: resolve monomodal_code for SP-type stops.
-- The numeric part of a StopArea:SP URI is the monomodal_code directly.
-- Join against dim_monomodal_stop to guard against codes absent from stations.csv.
UPDATE core.dim_stop ds
SET monomodal_code = CAST(substring(ds.stop_id FROM 'STIF:StopArea:SP:(\d+):') AS INTEGER)
FROM core.dim_monomodal_stop ms
WHERE ds.stop_type      = 'SP'
  AND ds.monomodal_code IS NULL
  AND CAST(substring(ds.stop_id FROM 'STIF:StopArea:SP:(\d+):') AS INTEGER) = ms.monomodal_code;
