-- Step 1: Create a staging table with the same structure as the original layoffs table
CREATE TABLE layoff_staging LIKE layoffs;

-- Step 2: Insert all data from the layoffs table into the staging table
INSERT INTO layoff_staging
SELECT * 
FROM layoffs;

-- Step 3: Identify duplicates by assigning a row number based on key fields
SELECT *,
ROW_NUMBER() OVER (
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions
) AS row_num
FROM layoff_staging;

-- Step 4: Use a CTE to filter out duplicate rows
WITH duplicate_cte AS (
  SELECT *,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions
  ) AS row_num
  FROM layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Step 5: Create a new staging table for cleaned data
CREATE TABLE layoff_staging2 (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Step 6: Insert data into the new staging table with row numbers
INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER (
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions
) AS row_num
FROM layoff_staging;

-- Step 7: Trim whitespace from the 'company' column
UPDATE layoff_staging2
SET company = TRIM(company);

-- Step 8: Standardize the 'industry' column (e.g., correct capitalization)
UPDATE layoff_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- Step 9: Correct country names (e.g., replacing incorrect or politically sensitive names)
UPDATE layoff_staging2 
SET country = 'Palestine'
WHERE country LIKE 'israel';

-- Step 10: Remove trailing periods from country names
UPDATE layoff_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Step 11: Convert 'date' strings to a standardized date format
UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

-- Step 12: Handle missing data by setting empty fields to NULL
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

-- Step 13: Delete rows with null values in key columns
DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Step 14: Drop the 'row_num' column after cleaning
ALTER TABLE layoff_staging2
DROP COLUMN row_num;

-- Step 15: Exploratory Data Analysis

-- Calculate the maximum values for total layoffs and percentage laid off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_staging2;

-- Summarize layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Analyze date ranges
SELECT MIN(`date`), MAX(`date`)
FROM layoff_staging2;

-- Layoffs by year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Monthly analysis of layoffs
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoff_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Rolling totals of layoffs over months
WITH rolling_totals AS (
  SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
  FROM layoff_staging2
  WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
  GROUP BY `MONTH`
  ORDER BY 1 ASC
)
SELECT `MONTH`, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_over
FROM rolling_totals;

-- Rank companies by layoffs per year
WITH company_year AS (
  SELECT company, YEAR(`date`), SUM(total_laid_off)
  FROM layoff_staging2
  GROUP BY company, YEAR(`date`)
), company_year_rank AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM company_year
  WHERE years IS NOT NULL
)
SELECT * 
FROM company_year_rank;
