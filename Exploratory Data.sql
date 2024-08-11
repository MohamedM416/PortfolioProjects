-- 1. Calculate Maximum Values
-- Get the maximum number of layoffs and the maximum percentage laid off across all records
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_staging2;

-- 2. Total Layoffs by Company
-- Summarize total layoffs for each company and order the results by the highest totals
SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

-- 3. Date Range Analysis
-- Find the earliest and latest dates in the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoff_staging2;

-- 4. Annual Layoffs Summary
-- Aggregate the total layoffs per year and order by year in descending order
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`) DESC;

-- 5. Monthly Layoffs Summary
-- Aggregate the total layoffs by month and order by month in ascending order
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoff_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

-- 6. Rolling Total of Layoffs Over Time
-- Calculate a rolling total of layoffs by month
WITH rolling_totals AS (
  SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
  FROM layoff_staging2
  WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
  GROUP BY `MONTH`
  ORDER BY `MONTH` ASC
)
SELECT `MONTH`, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_over
FROM rolling_totals;

-- 7. Layoffs by Company (Repeated Query)
-- Summarize total layoffs by company and order by highest totals (same as step 2)
SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

-- 8. Layoffs by Company and Year
-- Aggregate layoffs by both company and year
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company, YEAR(`date`);

-- 9. Ranking Companies by Yearly Layoffs
-- Rank companies within each year by total layoffs
WITH company_year AS (
  SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoff_staging2
  GROUP BY company, years
), company_year_rank AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM company_year
  WHERE years IS NOT NULL
)
SELECT * 
FROM company_year_rank;
