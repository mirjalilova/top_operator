CREATE TABLE operators (
    id INT PRIMARY KEY DEFAULT gen_random_uuid(),
    login INT NOT NULL,
    full_name VARCHAR NOT NULL,
    group_name VARCHAR NOT NULL,
    avatar_url VARCHAR,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE operator_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    operator_id INT NOT NULL,
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


CREATE OR REPLACE FUNCTION resolve_cycle(p_date DATE)
RETURNS TABLE(year INT, month INT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXTRACT(DAY FROM p_date) >= 20 THEN
        year  := EXTRACT(YEAR FROM p_date);
        month := EXTRACT(MONTH FROM p_date);
    ELSE
        IF EXTRACT(MONTH FROM p_date) = 1 THEN
            year  := EXTRACT(YEAR FROM p_date) - 1;
            month := 12;
        ELSE
            year  := EXTRACT(YEAR FROM p_date);
            month := EXTRACT(MONTH FROM p_date) - 1;
        END IF;
    END IF;
    RETURN NEXT;
END;
$$;



CREATE OR REPLACE FUNCTION recalc_operator_monthly_metric(
    p_operator_id INT,
    p_year INT,
    p_month INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_call_count INT;
    v_avg_busy FLOAT;
    v_kpi FLOAT;
BEGIN
    SELECT COALESCE(SUM(call_count), 0)
    INTO v_call_count
    FROM operator_metrics
    WHERE operator_uuid = p_operator_id
      AND date >= make_date(p_year, p_month, 20) - INTERVAL '1 month'
      AND date <  make_date(p_year, p_month, 20);

    SELECT AVG(
        CASE 
            WHEN call_count > 0
            THEN EXTRACT(EPOCH FROM busy_duration::interval) / call_count
        END
    )
    INTO v_avg_busy
    FROM operator_metrics
    WHERE operator_uuid = p_operator_id
      AND date >= make_date(p_year, p_month, 20) - INTERVAL '1 month'
      AND date <  make_date(p_year, p_month, 20)
      AND call_count > 0;

    -- KPI → 20-sana
    SELECT kpi
    INTO v_kpi
    FROM operator_metrics
    WHERE operator_uuid = p_operator_id
      AND date = make_date(p_year, p_month, 20)
    LIMIT 1;

    INSERT INTO operator_monthly_metrics (
        operator_uuid,
        year,
        month,
        call_count,
        avg_busy_per_call,
        kpi
    )
    VALUES (
        p_operator_id,
        p_year,
        p_month,
        COALESCE(v_call_count, 0),
        COALESCE(v_avg_busy, 0),
        COALESCE(v_kpi, 0)
    )
    ON CONFLICT (operator_uuid, year, month)
    DO UPDATE SET
        call_count = EXCLUDED.call_count,
        avg_busy_per_call = EXCLUDED.avg_busy_per_call,
        kpi = EXCLUDED.kpi;
END;
$$;



CREATE OR REPLACE FUNCTION trg_daily_metric_to_monthly()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_year INT;
    v_month INT;
BEGIN
    SELECT year, month
    INTO v_year, v_month
    FROM resolve_cycle(NEW.date);

    PERFORM recalc_operator_monthly_metric(
        NEW.operator_uuid,
        v_year,
        v_month
    );

    RETURN NEW;
END;
$$;



CREATE TRIGGER trg_operator_metrics_monthly
AFTER INSERT OR UPDATE ON operator_metrics
FOR EACH ROW
EXECUTE FUNCTION trg_daily_metric_to_monthly();


CREATE OR REPLACE FUNCTION finalize_monthly_ranking(p_year INT, p_month INT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    rank_pos INT := 0;
    score_val INT;
BEGIN
    FOR r IN
        SELECT id
        FROM operator_monthly_metrics
        WHERE year = p_year AND month = p_month
        ORDER BY kpi DESC
        LIMIT 10
    LOOP
        rank_pos := rank_pos + 1;
        score_val := 1100 - (rank_pos * 100);

        UPDATE operator_monthly_metrics
        SET
            rank = rank_pos,
            score = score_val,
            is_top_1 = (rank_pos = 1),
            stars = CASE
                WHEN rank_pos = 1 THEN 5
                WHEN rank_pos <= 3 THEN 4
                WHEN rank_pos <= 5 THEN 3
                ELSE 2
            END
        WHERE id = r.id;
    END LOOP;
END;
$$;
