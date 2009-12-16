select jj.status, count(*)
from jobs_job jj
left join jobs_job_assets jja on jj.id = jja.job_id
where jja.asset_id in
(select id from assets_asset where instrument_name = 'MOC-WA')
group by jj.status
;

update jobs_job
    set status = 'cancelled'
    where id in
        (
        select jj.id
        from jobs_job jj
        left join
            jobs_job_assets jja on jj.id = jja.job_id
                left join assets_asset aa on jja.asset_id = aa.id
        where aa.instrument_name = 'MOC-WA'
        and jj.status != 'complete')
        ;
        

select aa.instrument_name, jj.status, count(*)
from jobs_job jj
left join
    jobs_job_assets jja on jj.id = jja.job_id
        left join assets_asset aa on jja.asset_id = aa.id
where aa.instrument_name is not null
group by aa.instrument_name, jj.status
order by aa.instrument_name, jj.status
;