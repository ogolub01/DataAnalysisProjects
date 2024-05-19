
-- MySQL DATA CLEANING PROJECT

SELECT *
FROM layoffs;

-- Creating a copy of the original table (backup)
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- REMOVING DUPLICATES
-- Creating row numbers to match against columns as an identifying factor. 
-- Partitioning by company, industrym total_laid_off, percentage_laid_off and date.
-- row_num values greater than 1 indicate duplicates.


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicates_cte
WHERE row_num > 1;

-- Double checking if rows are duplicates.

SELECT *
FROM layoffs_staging
WHERE company = 'Shopee';

-- Not all values are duplicates. So, I am going to partition by every column.

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off
, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicates_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Better.com';


-- Deleting duplicates by creating another table that has extra row, 
-- then deleting it where the row is greater than 1.

-- (right-click on layoff_staging table > copy to clipboard > create statement)
-- (changing table name by adding version number 2 and adding row_num column)
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off
, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Checking only duplicates are selected before deleting!
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- One last check that duplicates are gone.

-- STANDARDISING DATA

-- Dealing with blank spaces around company names.
SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Industry cleanup
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Some NULL/blank spaces. In addition, Crypto, Cryptocurrency and Crypto Currency 
-- should all have the same label.

SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- United States. and United States - needs fixing

SELECT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Dealing with date being text column
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- WORKING WITH NULL AND BLANK VALUES

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- Ideally we would want to find the missing values via other means (e.g. web scraping),
-- but I will be using this data for another exploratory project and replacing the missing values
-- is beyond the scope of this one :)

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;