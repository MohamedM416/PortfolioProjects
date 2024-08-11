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

