-- The table must exist before the load: its column types are the ones used for the CSV values.
CREATE TABLE cosmotech_results (
    id          bigint,
    created_at  timestamp,
    category    text,
    -- Only needed with --add-id: filled with the value of --run-id
    csm_run_id  text NOT NULL DEFAULT current_setting('csm.run_id')
);
