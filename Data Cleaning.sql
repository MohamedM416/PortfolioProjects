 -- Data Cleaning

CREATE TABLE layoff_staging
like layoffs;

select * 
from layoff_staging;

insert layoff_staging
select * 
from layoffs;


select *,
row_Number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) As row_num
from layoff_staging;

with duplicate_cte as
(
select *,
row_Number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions ) As row_num
from layoff_staging
)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoff_staging2;

insert into layoff_staging2
select *,
row_Number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions ) As row_num
from layoff_staging;

select *
from layoff_staging2 
where row_num > 1; 

update layoff_staging2
set company = trim(company);

update layoff_staging2 
set industry = 'Crypto'
where industry like 'crypto%';

update layoff_staging2 
set country = 'plastine'
where country like 'israel';

select distinct country, trim(trailing '.' from country)
from layoff_staging2
order by 1;

update layoff_staging2 
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

Alter table layoff_staging2
modify column `date` DATE;
select * 
from layoff_staging2;

select *
from layoff_staging2 
where total_laid_off is null
and percentage_laid_off is null;

update layoff_staging2
set industry = null
where industry = '';

select * 
from layoff_staging2 
where industry is null
or industry = '';

select * 
from layoff_staging2
where company Like 'Bally%';

select *
from layoff_staging2 t1
join layoff_staging2 t2 
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_staging2 t1
join layoff_staging2 t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select *
from layoff_staging2 
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoff_staging2 
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoff_staging2 ;

alter table layoff_staging2
drop column row_num;
 -- Exploratory Data
select max(total_laid_off), max(percentage_laid_off)
from layoff_staging2;

select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select MIN(`date`),MAX(`date`)
from layoff_staging2;

select year(`date`), sum(total_laid_off)
from layoff_staging2
group by year(`date`)
order by 1 desc;

select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
from layoff_staging2
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 ASC;

with rolling_totale as
(
select substring(`date`,1,7) as `MONTH`, sum(total_laid_off) as total_off
from layoff_staging2
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 ASC
)
select `MONTH`, sum(total_off) over(order by `MONTH`) as rolling_over
from rolling_totale;

select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select company, year(`date`), sum(total_laid_off)
from layoff_staging2
group by company, year(`date`);

with company_year (company,years,total_laid_off) as 
(
select company, year(`date`), sum(total_laid_off)
from layoff_staging2
group by company, year(`date`)
), company_year_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) As rainking
from company_year
where years is not null
)
select * 
from company_year_rank
;

