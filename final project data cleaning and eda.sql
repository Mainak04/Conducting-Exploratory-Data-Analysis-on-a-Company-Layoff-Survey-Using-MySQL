--              MYSQL Project on data cleaning and Exploratory Data analyis

--         database: world_layoffs(information about layoffs by different companies)

use world_layoffs;
select * from layoffs;

-- Data cleaning project
-- 1) removing duplicates
-- 2) standardizing data
-- 3) checking for null and zero values
-- 4) removing columns if required


-- creating a separate table for raw data

create table layoffs_staging
like layoffs;

insert into layoffs_staging 
select* from layoffs;

-- removing duplicates
with duplicate_cte as
(select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num  from layoffs_staging)
select* from duplicate_cte where row_num>1;

with duplicate_cte as
(select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num  from layoffs_staging)
delete from duplicate_cte where row_num>1;

-- error as we can not update a cte for eg. we can not delete
-- we moved the subquery data into a new table and deleted duplicate rows


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
 
 insert into layoffs_staging2
 select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num  from layoffs_staging;
 
 
 select * from layoffs_staging2;
 
 delete from layoffs_staging2 where row_num>1;
 
 select* from layoffs_staging2;
 
 -- standardizing data
 
 select distinct(trim(company))
 from layoffs_staging2;
 
 update layoffs_staging2
 set company=trim(company);

-- company names are trimmed 

select distinct(industry) 
from layoffs_staging2
order by 1;

-- crypto and crypto currency are the same thing so standardizing it to crypto

 update layoffs_staging2
 set industry='Crypto'
 where industry like 'Crypto%';
 
 select distinct country
 from layoffs_staging2
 order by 1;
 
-- united states and united states. are the same thing so standardizing it to united states

update layoffs_staging2
set country= trim(trailing '.' from country)
 where country like 'United States%';

 
 -- working with null values
 
 select* from layoffs_staging2
 where industry is null or industry='';
 
 -- now we have to see that if any other row of same company and same location contains the required industry information so we can put that in place of null value or blank values
 select*
 from layoffs_staging2 as t1
 join layoffs_staging2 as t2
 on t1.company=t2.company
 and t1.location=t2.location
 where (t1.industry is null or t1.industry='')
 and t2.industry is not null;
 
-- using join we will fill the null or ''  industry values with appropriate industry value
update layoffs_staging2 as t1
set t1.industry= null
where t1.industry='';
-- to run the next query as '' was creating some problems so converted them to null values

update layoffs_staging2 as t1
join layoffs_staging2 as t2
on t1.company=t2.company
and t1.location=t2.location
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select company, location, industry from layoffs_staging2;

-- removing rows and columns
select* from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

--  this rows don't make any sense as both of the column values are null and we are looking at a layoff report

delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- now we don't need the column row_num

alter table layoffs_staging2
drop column row_num;

select* from layoffs_staging2;

-- this is the final cleaned data
--                                    ~ end of cleaning project ~

-- Exploratory data analysis project
 -- studying the trends and patterns from the data
 
 select * from layoffs_staging2;
 
 select max(total_laid_off), max(percentage_laid_off)
 from layoffs_staging2;
 
 -- max no of employees that were laid off in a day are 12000 and some companies laid off their all employees
 
 select company, total_laid_off from
 layoffs_staging2 where 
 percentage_laid_off=1 
 order by total_laid_off desc; 
 
 -- Katerra was the biggest company( in terms of no of employees) which laid off all their employees in a day
 
 select company, funds_raised_millions from
 layoffs_staging2 where 
 percentage_laid_off=1 
 order by funds_raised_millions desc; 
 
 -- Britishvolt was the biggest company( in terms of funds raised) which laid off all their employees in a day
 
 select company, sum(total_laid_off)
 from layoffs_staging2
 group by company
 order by 2 desc;
 
 -- amazon laid off the most no of employees in total

 
 select industry, sum(total_laid_off)
 from layoffs_staging2
 group by industry
 order by 2 desc;
 
 -- consumer sector was hit the most therefore more employees were laid off
 
 select country, sum(total_laid_off)
 from layoffs_staging2
 group by country
 order by 2 desc;
 
 -- employees in united states were laid off the most in this time span 
 
  select stage, sum(total_laid_off)
 from layoffs_staging2
 group by stage
 order by 2 desc;
 
 -- Post Ipo had the most employees laid off
 
  select company, avg(percentage_laid_off)
 from layoffs_staging2
 group by company
 order by 2 desc;
 
 --                                ~ end of eda project ~
