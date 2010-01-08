ALTER TABLE jobs_job ADD COLUMN transaction_id integer NULL;
CREATE SEQUENCE seq_transaction_id;

CREATE TABLE "jobs_job_dependencies" (
    "id" integer NOT NULL PRIMARY KEY,
    "from_job_id" integer NOT NULL REFERENCES "jobs_job" ("id"),
    "to_job_id" integer NOT NULL REFERENCES "jobs_job" ("id"),
    UNIQUE ("from_job_id", "to_job_id")
);

ALTER TABLE jobs_jobset ADD COLUMN "priority" integer NOT NULL DEFAULT 0;
