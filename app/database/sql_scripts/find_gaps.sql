-- Type to hold gap results
DO $$ BEGIN
    CREATE TYPE gap_range AS (gap_start TIMESTAMPTZ, gap_end TIMESTAMPTZ);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- Main entry point: finds gaps and merges adjacent ones
CREATE OR REPLACE FUNCTION find_ohlcv_gaps(
    p_start_date    TIMESTAMPTZ,
    p_end_date      TIMESTAMPTZ,
    p_timeframe     TEXT,            -- '1h', '1m', '1s', '1d', etc.
    p_exchange      TEXT,
    p_symbol        TEXT,
    p_check_right   BOOLEAN DEFAULT TRUE
)
RETURNS SETOF gap_range
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_interval      INTERVAL;
    v_raw_gaps      gap_range[];
    v_merged        gap_range[];
    v_prev          gap_range;
    v_curr          gap_range;
    i               INT;
BEGIN
    v_interval := _timeframe_to_interval(p_timeframe);

    -- Collect timestamps present in the table
    -- Pass them into the recursive worker
    SELECT _find_gaps_recursive(
        p_start_date,
        p_end_date,
        v_interval,
        ARRAY(
            SELECT timestamp
            FROM ohlcv
            WHERE exchange  = p_exchange
              AND symbol    = p_symbol
              AND timeframe = p_timeframe
              AND timestamp >= p_start_date
              AND timestamp <= p_end_date
            ORDER BY timestamp
        ),
        p_check_right
    ) INTO v_raw_gaps;

    -- Merge adjacent/overlapping gaps
    IF array_length(v_raw_gaps, 1) IS NULL THEN
        RETURN;
    END IF;

    v_merged := ARRAY[v_raw_gaps[1]];

    FOR i IN 2 .. array_length(v_raw_gaps, 1) LOOP
        v_curr := v_raw_gaps[i];
        v_prev := v_merged[array_length(v_merged, 1)];
        IF v_curr.gap_start <= v_prev.gap_end + v_interval THEN
            -- Extend previous gap
            v_merged[array_length(v_merged, 1)] :=
                ROW(v_prev.gap_start, v_curr.gap_end)::gap_range;
        ELSE
            v_merged := v_merged || v_curr;
        END IF;
    END LOOP;

    IF v_merged IS NOT NULL THEN
        FOR i IN 1 .. array_length(v_merged, 1) LOOP
            -- Only return gaps that have a duration
            IF v_merged[i].gap_start < v_merged[i].gap_end THEN
                RETURN NEXT v_merged[i];
            END IF;
        END LOOP;
    END IF;

    FOR i IN 1 .. array_length(v_merged, 1) LOOP
        RETURN NEXT v_merged[i];
    END LOOP;
END;
$$;


-- Recursive worker
-- Operates on sorted timestamp arrays instead of integer arrays
CREATE OR REPLACE FUNCTION _find_gaps_recursive(
    p_start         TIMESTAMPTZ,
    p_end           TIMESTAMPTZ,
    p_interval      INTERVAL,
    p_timestamps    TIMESTAMPTZ[],
    p_check_right   BOOLEAN
)
RETURNS gap_range[]
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_expected_count BIGINT;
    v_present_count  INT;
    v_half_ts        TIMESTAMPTZ;
    v_half_offset    BIGINT;
    v_left           TIMESTAMPTZ[];
    v_right          TIMESTAMPTZ[];
BEGIN
    v_expected_count := _count_intervals(p_start, p_end, p_interval);
    v_present_count  := array_length(p_timestamps, 1);

    IF v_present_count IS NULL THEN
        v_present_count := 0;
    END IF;

    -- Base case: fully filled
    IF v_present_count = v_expected_count THEN
        RETURN ARRAY[]::gap_range[];
    END IF;

    -- Base case: completely empty
    IF v_present_count = 0 THEN
        RETURN ARRAY[ROW(p_start, p_end)::gap_range];
    END IF;

    -- Compute biased split point
    IF p_check_right THEN
        v_half_offset := v_expected_count - v_present_count;
    ELSE
        v_half_offset := v_present_count;
    END IF;

    -- Clamp: at least 1 step from start, at most (expected-1) steps from start
    v_half_offset := GREATEST(1, LEAST(v_half_offset, v_expected_count - 1));
    v_half_ts     := p_start + (v_half_offset - 1) * p_interval;

    -- Split timestamps into left (<= half) and right (> half)
    SELECT
        COALESCE(array_agg(t ORDER BY t) FILTER (WHERE t <= v_half_ts), ARRAY[]::TIMESTAMPTZ[]),
        COALESCE(array_agg(t ORDER BY t) FILTER (WHERE t >  v_half_ts), ARRAY[]::TIMESTAMPTZ[])
    INTO v_left, v_right
    FROM unnest(p_timestamps) AS t;

    RETURN _find_gaps_recursive(p_start, v_half_ts, p_interval, v_left,  NOT p_check_right)
        || _find_gaps_recursive(v_half_ts + p_interval, p_end, p_interval, v_right, NOT p_check_right);
END;
$$;


-- Convert timeframe string like '1h', '5m', '1s', '1d' to INTERVAL
CREATE OR REPLACE FUNCTION _timeframe_to_interval(p_timeframe TEXT)
RETURNS INTERVAL
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_num  INT;
    v_unit TEXT;
BEGIN
    v_num  := (regexp_match(p_timeframe, '^(\d+)'))[1]::INT;
    v_unit := (regexp_match(p_timeframe, '(\D+)$'))[1];

    RETURN CASE v_unit
        WHEN 's'   THEN make_interval(secs  => v_num)
        WHEN 'm'   THEN make_interval(mins  => v_num)
        WHEN 'h'   THEN make_interval(hours => v_num)
        WHEN 'd'   THEN make_interval(days  => v_num)
        WHEN 'w'   THEN make_interval(weeks => v_num)
        WHEN 'M'   THEN make_interval(months => v_num)
        ELSE             p_timeframe::INTERVAL  -- fallback: let PG parse it
    END;
END;
$$;


-- Count how many intervals fit in [start, end] inclusive
CREATE OR REPLACE FUNCTION _count_intervals(
    p_start    TIMESTAMPTZ,
    p_end      TIMESTAMPTZ,
    p_interval INTERVAL
)
RETURNS BIGINT
LANGUAGE SQL IMMUTABLE
AS $$
    SELECT EXTRACT(EPOCH FROM (p_end - p_start))::BIGINT
         / EXTRACT(EPOCH FROM p_interval)::BIGINT
    + 1;
$$;


-- ============================================================
-- Usage examples
-- ============================================================

-- Find all gaps in BTC/USDT 1h data for January 2020:
--
--   SELECT * FROM find_ohlcv_gaps(
--       '2020-01-01'::timestamptz,
--       '2020-02-01'::timestamptz,
--       '1h',
--       'binance',
--       'BTC/USDT'
--   );
--
-- With right-side bias disabled:
--
--   SELECT * FROM find_ohlcv_gaps(
--       '2020-01-01'::timestamptz,
--       '2020-02-01'::timestamptz,
--       '1h',
--       'binance',
--       'BTC/USDT',
--       FALSE
--   );