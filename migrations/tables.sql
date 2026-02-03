CREATE TABLE operators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    operator_id VARCHAR NOT NULL,
    agent_id INT,
    full_name VARCHAR NOT NULL,
    group_name VARCHAR NOT NULL,
    avatar_url VARCHAR,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE operator_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    operator_uuid UUID NOT NULL,
    date DATE NOT NULL,

    busy_duration VARCHAR,
    call_count DOUBLE PRECISION,
    distributed_call_count DOUBLE PRECISION,
    full_duration VARCHAR,
    hold_duration VARCHAR,
    idle_duration VARCHAR,
    lock_duration VARCHAR,

    kpi DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT now(),

    CONSTRAINT fk_operator_metrics_operator
        FOREIGN KEY (operator_uuid)
        REFERENCES operators(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_operator_metrics_operator_date
        UNIQUE (operator_uuid, date)
);

CREATE INDEX idx_operator_metrics_operator_uuid
ON operator_metrics (operator_uuid);

-- moth insert
CREATE TABLE operator_monthly_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    operator_uuid UUID NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,

    call_count INT,
    avg_busy_per_call FLOAT,
    kpi FLOAT,

    rank INT,
    score INT,
    is_top_1 BOOLEAN DEFAULT FALSE,
    stars INT,

    created_at TIMESTAMP DEFAULT now(),

    CONSTRAINT fk_operator_monthly_metrics_operator
        FOREIGN KEY (operator_uuid)
        REFERENCES operators(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_operator_monthly_metrics_operator_year_month
        UNIQUE (operator_uuid, year, month)
);


CREATE TABLE bonus_distributions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    operator_uuid UUID NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    kie INT,
    active_participation INT,
    monitoring INT,

    CONSTRAINT fk_bonus_distributions_operator
        FOREIGN KEY (operator_uuid)
        REFERENCES operators(id)
        ON DELETE CASCADE
);

CREATE TABLE operator_daily_rank (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    operator_uuid UUID NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    date DATE NOT NULL,

    rank INT NOT NULL,

    created_at TIMESTAMP DEFAULT now(),

    UNIQUE (operator_uuid, date),
    FOREIGN KEY (operator_uuid) REFERENCES operators(id)
);


CREATE OR REPLACE FUNCTION resolve_cycle(p_date DATE)
RETURNS TABLE(year INT, month INT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXTRACT(DAY FROM p_date) < 20 THEN
        year  := EXTRACT(YEAR FROM p_date);
        month := EXTRACT(MONTH FROM p_date);
    ELSE
        IF EXTRACT(MONTH FROM p_date) = 12 THEN
            year  := EXTRACT(YEAR FROM p_date) + 1;
            month := 1;
        ELSE
            year  := EXTRACT(YEAR FROM p_date);
            month := EXTRACT(MONTH FROM p_date) + 1;
        END IF;
    END IF;

    RETURN NEXT;
END;
$$;


CREATE OR REPLACE FUNCTION ensure_operator_monthly_row()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_year  INT;
    v_month INT;
BEGIN
    SELECT year, month
    INTO v_year, v_month
    FROM resolve_cycle(NEW.date);

    INSERT INTO operator_monthly_metrics (
        operator_uuid,
        year,
        month,
        call_count,
        avg_busy_per_call,
        kpi,
        rank,
        score,
        is_top_1,
        stars
    )
    VALUES (
        NEW.operator_uuid,
        v_year,
        v_month,
        0,
        0,
        NULL,
        NULL,
        NULL,
        FALSE,
        NULL
    )
    ON CONFLICT (operator_uuid, year, month)
    DO NOTHING;

    RETURN NEW;
END;
$$;


CREATE TRIGGER trg_ensure_monthly_row
AFTER INSERT ON operator_metrics
FOR EACH ROW
EXECUTE FUNCTION ensure_operator_monthly_row();



-- 1
CREATE OR REPLACE FUNCTION recalc_operator_monthly_metrics_daily(
    p_operator_uuid UUID,
    p_year  INT,
    p_month INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_date DATE;
    v_end_date   DATE;
    v_call_count INT;
    v_avg_busy   FLOAT;
    v_kpi        FLOAT;
BEGIN

    IF p_month = 1 THEN
        v_start_date := make_date(p_year - 1, 12, 20);
    ELSE
        v_start_date := make_date(p_year, p_month - 1, 20);
    END IF;

    v_end_date := make_date(p_year, p_month, 20);

    SELECT COALESCE(SUM(call_count), 0)
    INTO v_call_count
    FROM operator_metrics
    WHERE operator_uuid = p_operator_uuid
      AND date >= v_start_date
      AND date <  v_end_date;

    SELECT AVG(
        EXTRACT(EPOCH FROM busy_duration::interval) / call_count
    )
    INTO v_avg_busy
    FROM operator_metrics
    WHERE operator_uuid = p_operator_uuid
      AND date >= v_start_date
      AND date <  v_end_date
      AND call_count > 0;

    SELECT kpi
    INTO v_kpi
    FROM operator_metrics
    WHERE operator_uuid = p_operator_uuid
      AND date >= v_start_date
      AND date <  v_end_date
      AND kpi IS NOT NULL
    ORDER BY date DESC
    LIMIT 1;

    UPDATE operator_monthly_metrics
    SET
        call_count = v_call_count,
        avg_busy_per_call = COALESCE(v_avg_busy, 0),
        kpi = v_kpi
    WHERE operator_uuid = p_operator_uuid
      AND year = p_year
      AND month = p_month;
END;
$$;

CREATE OR REPLACE FUNCTION trg_update_monthly_metrics_daily()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_year  INT;
    v_month INT;
BEGIN
    SELECT year, month
    INTO v_year, v_month
    FROM resolve_cycle(NEW.date);

    PERFORM recalc_operator_monthly_metrics_daily(
        NEW.operator_uuid,
        v_year,
        v_month
    );

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_operator_metrics_to_monthly_update
AFTER INSERT OR UPDATE ON operator_metrics
FOR EACH ROW
EXECUTE FUNCTION trg_update_monthly_metrics_daily();





-- 2

CREATE OR REPLACE FUNCTION finalize_monthly_scores(
    p_year  INT,
    p_month INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    WITH base AS (
        SELECT
            m.id,
            m.operator_uuid,
            o.group_name,

            COALESCE(m.call_count, 0)         AS call_count,
            COALESCE(m.kpi, 0)                AS kpi,
            COALESCE(m.avg_busy_per_call, 0) AS avg_busy_per_call
        FROM operator_monthly_metrics m
        JOIN operators o ON o.id = m.operator_uuid
        WHERE m.year = p_year
          AND m.month = p_month
    ),

    stats AS (
        SELECT
            *,
            MIN(call_count) OVER (PARTITION BY group_name) AS min_call,
            MAX(call_count) OVER (PARTITION BY group_name) AS max_call,

            MIN(kpi) OVER (PARTITION BY group_name) AS min_kpi,
            MAX(kpi) OVER (PARTITION BY group_name) AS max_kpi,

            MIN(avg_busy_per_call) OVER (PARTITION BY group_name) AS min_avg,
            MAX(avg_busy_per_call) OVER (PARTITION BY group_name) AS max_avg
        FROM base
    ),

    normalized AS (
        SELECT
            id,
            operator_uuid,
            group_name,

            CASE
                WHEN max_call = min_call THEN 0
                ELSE (call_count - min_call)::FLOAT / (max_call - min_call)
            END AS count_norm,

            CASE
                WHEN max_kpi = min_kpi THEN 0
                ELSE (kpi - min_kpi)::FLOAT / (max_kpi - min_kpi)
            END AS kpi_norm,

            CASE
                WHEN max_avg = min_avg THEN 0
                ELSE (max_avg - avg_busy_per_call)::FLOAT / (max_avg - min_avg)
            END AS avg_norm
        FROM stats
    ),

    scored AS (
        SELECT
            id,
            operator_uuid,
            group_name,
            (0.5 * count_norm
           + 0.1 * kpi_norm
           + 0.4 * avg_norm) AS total_score
        FROM normalized
    ),

    ranked AS (
        SELECT
            id,
            operator_uuid,
            group_name,
            total_score,
            DENSE_RANK() OVER (
                PARTITION BY group_name
                ORDER BY total_score DESC
            ) AS rank
        FROM scored
    )

    UPDATE operator_monthly_metrics m
    SET
        rank = r.rank,

        score = CASE
            WHEN r.rank = 1 THEN 1000
            WHEN r.rank = 2 THEN 900
            WHEN r.rank = 3 THEN 800
            WHEN r.rank = 4 THEN 700
            WHEN r.rank = 5 THEN 600
            WHEN r.rank = 6 THEN 500
            WHEN r.rank = 7 THEN 400
            WHEN r.rank = 8 THEN 300
            WHEN r.rank = 9 THEN 200
            WHEN r.rank = 10 THEN 100
            ELSE 0
        END,

        is_top_1 = (r.rank <= 3),

        stars = CASE
            WHEN r.rank = 1 THEN 3
            WHEN r.rank = 2 THEN 2
            WHEN r.rank = 3 THEN 1
            ELSE 0
        END
    FROM ranked r
    WHERE m.id = r.id;
END;
$$;








-- 3
CREATE OR REPLACE FUNCTION snapshot_daily_rank(
    p_year INT,
    p_month INT,
    p_date DATE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO operator_daily_rank (
        operator_uuid,
        year,
        month,
        date,
        rank    )
    SELECT
        operator_uuid,
        p_year,
        p_month,
        p_date,
        rank
    FROM operator_monthly_metrics
    WHERE year = p_year
      AND month = p_month
      AND rank IS NOT NULL
    ON CONFLICT (operator_uuid, date)
    DO UPDATE SET
        rank  = EXCLUDED.rank,
        created_at = now();
END;
$$;





CREATE INDEX idx_operator_daily_rank_lookup
ON operator_daily_rank (operator_uuid, year, month, date);
