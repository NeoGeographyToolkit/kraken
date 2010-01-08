ALTER TABLE jobs_job ADD COLUMN "time_started" timestamp with time zone NULL;
ALTER TABLE jobs_job ADD COLUMN "time_ended" timestamp with time zone NULL;
ALTER TABLE jobs_job ADD COLUMN "pid" integer NULL;
ALTER TABLE dispatch_reaper ADD COLUMN "current_job_id" integer NULL REFERENCES "jobs_job" ("id");
