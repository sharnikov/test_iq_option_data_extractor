# test_iq_option_data_extractor

Scripts to create database table:
```
create table VACANCIES(
    id TEXT not null,
    premium boolean not null,
    name TEXT NOT NULL,
    department_id TEXT,
    department_name TEXT,
    has_test BOOLEAN NOT NULL,
    response_letter_required BOOLEAN NOT NULL,
    area_id TEXT NOT NULL,
    area_name TEXT NOT NULL,
    salary_from INTEGER,
    salary_to INTEGER,
    salary_currency TEXT,
    salary_gross BOOLEAN,
    is_open BOOLEAN NOT NULL,
    adress_id TEXT,
    employer_id TEXT,
    employer_name TEXT,
    created_at DATE,
    url TEXT,
    alternate_url TEXT,
    snippet_requirement TEXT,
    snippet_responsibility TEXT
);
```

create index vacancies_date_index on vacancies(created_at);
cluster vacancies using vacancies_date_index;

create index vacancy_is_open_index on vacancies(is_open);
create index vacancy_id_index on vacancies(id);

alter table vacancies add primary key (id);

alter table vacancies alter column is_open set default true;


Schemas to build the scripts to do potential analytics:

select avg(
case when(salary_currency = 'RUR') THEN salary_from
     when(salary_currency = 'EUR') THEN salary_from*70
     when(salary_currency = 'USD') THEN salary_from*60
END
) from vacancies where salary_from is not null and salary_currency in ('RUR', 'USD', 'EUR');


- select min/max/avg(salary_from/salary_to) from vacancies where [is_open = true/false] and salary_gross = true/false and
salary_from/salary_to is not null [and date between (date1, date2)] and salary_currency = RUR/EUR/USD [group by id];

- select count(*) from vacancies from vacancies [where is_open = true] [and date between (date1, date2)] group by id
- select * from vacancies from vacancies where is_open = true [and date between (date1, date2)]

- select min/max/avg(
case when(salary_currency = 'RUB') THEN salary_from/salary_to
     when(salary_currency = 'USD') THEN salary_from/salary_to*60
     when(salary_currency = 'EUR') THEN salary_from/salary_to*70
END
) from vacancies where [is_open = true/false] and salary_gross = true/false and
salary_from/salary_to is not null and salary_currency in ('RUR', 'USD', 'EUR') [and date between (date1, date2)] [group by id];
