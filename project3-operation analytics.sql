CREATE DATABASE project_3;
show databases;
USE project_3;

# TABLE 1- users

CREATE TABLE users(
	user_id INT,
	created_at VARCHAR(100),
	company_id INT,
	language VARCHAR (50),
	activated_at VARCHAR(100),
    state VARCHAR (50));

SHOW variables LIKE 'secure_file_priv' ;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Data/Case Study 2/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from users;

ALTER TABLE users ADD COLUMN created_date DATETIME;

alter table users;
update users set created_date = str_to_date(created_at , '%d-%m-%Y %H:%i');

ALTER TABLE users drop column created_at;

ALTER TABLE users CHANGE COLUMN created_date created_at DATETIME; 
select * from users;

ALTER TABLE users ADD COLUMN activated_date DATETIME;

alter table users;
update users set activated_date = str_to_date(activated_at , '%d-%m-%Y %H:%i');

ALTER TABLE users drop column activated_at;

ALTER TABLE users CHANGE COLUMN activated_date activated_at DATETIME; 
select * from users;

#TABLE 2 events

CREATE TABLE events(
	user_id	INT,
    occurred_at	VARCHAR(100),
    event_type VARCHAR(50),
	event_name VARCHAR (50),
	location VARCHAR (50),
    device VARCHAR (100),
    user_type INT
);

SHOW variables LIKE 'secure_file_priv' ;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Data/Case Study 2/events.csv'
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from events;

ALTER TABLE events ADD COLUMN occurred_date DATETIME;

alter table events;
update events set occurred_date = str_to_date(occurred_at , '%d-%m-%Y %H:%i');

ALTER TABLE events drop column occurred_at;

ALTER TABLE events CHANGE COLUMN occurred_date occurred_at DATETIME; 

select * from events;

#TABLE 3 email_events

CREATE TABLE email_events(
user_id INT,
occurred_at	VARCHAR (100),
action VARCHAR(100),
user_type INT
);

SHOW variables LIKE 'secure_file_priv' ;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Data/Case Study 2/email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from email_events;

#TABLE job_data

CREATE TABLE job_data(
ds DATE,
job_id INT,
actor_id INT,
event VARCHAR (50),
language VARCHAR (50),
time_spent INT,
org VARCHAR(5)
);

INSERT INTO job_data
VALUES ('2020-11-30', 21, 1001, 'skip',	'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian',	22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11,	'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian',	56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian',	45, 'C');

# Calculate the number of jobs reviewed per hour for each day in November 2020

 SELECT ds AS Date, 
 COUNT(job_id) AS Jobs_per_day,
 ROUND((SUM(time_spent)/3600),2) AS tot_time_spent_inhour,  
 ROUND((COUNT(job_id)/(SUM(time_spent)/3600)),2) AS Job_Rev_PHr_PDy 
 FROM job_data
 WHERE 
     ds BETWEEN '2020-11-01' AND '2020-11-30'
 GROUP BY ds 
 ORDER BY ds;
 
 
 #  Calculate the 7-day rolling average of throughput (number of events per second).
 # explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why. 
 WITH A AS (
 SELECT ds, CAST(COUNT(job_id) AS FLOAT)/CAST(SUM(time_spent) AS FLOAT) AS c_by_s
 FROM job_data
 WHERE ds BETWEEN '2020-11-01' and '2020-11-30'
 GROUP BY 1 )

 SELECT ds AS Date, 
 ROUND(c_by_s,2) AS Job_Rev_PSec_PDay, 
 round(AVG(c_by_s) OVER(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND  CURRENT ROW),2) AS 7_Day_Roll_Avg 
 FROM A;
 
 #  calculate the percentage share of each language over the last 30 days.
 
 WITH L AS (
 SELECT language, COUNT(language) AS lang_count
 FROM job_data 
 WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
 GROUP BY language )
        
 SELECT language AS Lang, lang_count, 
 ROUND((100*lang_count/SUM(lang_count) OVER()), 2) AS Perc_Lang 
 FROM L
 ORDER BY Perc_Lang;
 
 #  Identify duplicate rows in the data
 
 SELECT * FROM job_data
 GROUP BY ds, job_id, actor_id, event, language,  time_spent, org 
 HAVING COUNT(*)>1;
 
# Write an SQL query to calculate the weekly user engagement.
create view users_in_wk as
(select EXTRACT(WEEK FROM occurred_at) AS week_of_year, 
count(distinct user_id) as active_user,
dense_rank() over(order by EXTRACT(WEEK FROM occurred_at)) as week_number 
from events
where event_type = 'engagement'
group by week_of_year
);

# average users per week 
select ROUND(SUM(active_user)/COUNT(week_of_year),2) as Avg_Users_PWeek 
from users_in_wk;
 
# average weekly user engagement 
With W1 as (SELECT user_id, 
EXTRACT(WEEK FROM occurred_at) AS week_of_year, 
COUNT(user_id) AS Cnt 
 FROM events
 GROUP BY user_id, week_of_year 
 ORDER BY user_id)
SELECT ROUND(AVG(Cnt),2) AS Weekly_Eng_PUser
 FROM W1;
 
 select * from users_in_wk;

select count(distinct events.user_id) from events;

# calculate the user growth for the product.

create view weekly_new_users as(
SELECT TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', activated_at) AS WeekNumber, 
COUNT(user_id) AS WeeklyUserCount,
SUM(COUNT(user_id)) 
OVER (ORDER BY TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', activated_at)) AS CumulativeUsers
FROM users
GROUP BY TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', activated_at));

# calculate the user growth for the product.

create view monthly_new_users as(
SELECT TIMESTAMPDIFF(MONTH , '2013-01-01 04:40:10', activated_at) AS MonthNumber, 
COUNT(user_id) AS MonthlyUserCount,
SUM(COUNT(user_id)) 
OVER (ORDER BY TIMESTAMPDIFF(MONTH , '2013-01-01 04:40:10', activated_at)) AS CumulativeUsers
FROM users
GROUP BY TIMESTAMPDIFF(MONTH , '2013-01-01 04:40:10', activated_at));
select * from monthly_new_users;

# calculate the weekly retention of users based on their sign-up cohort.
CREATE VIEW Q5 AS (
    WITH cohorts AS (SELECT
	    TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', activated_at) AS cohort_start_week,
            COUNT(*) AS total_users
        FROM users
        GROUP BY 1
    ),
    weekly_stats AS (SELECT
            TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', activated_at) AS cohort_start_week,
            TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', occurred_at) AS engagement_week,
            COUNT(DISTINCT e.user_id) AS active_users
        FROM users u
        JOIN events e ON u.user_id = e.user_id
        WHERE e.event_type = 'engagement'
        GROUP BY 1, 2
    )
    SELECT
        cohorts.cohort_start_week,
        weekly_stats.engagement_week,
        weekly_stats.active_users,
        cohorts.total_users AS total_users,
        ROUND(weekly_stats.active_users / cohorts.total_users * 100, 2) AS retention_rate
    FROM cohorts
    JOIN weekly_stats ON cohorts.cohort_start_week = weekly_stats.cohort_start_week
    ORDER BY cohort_start_week, engagement_week);
select * from Q4;

# Average weekly retention rate.
select round(avg(retention_rate),2) as avg_weekly_retention_rate 
from Q4;

With CTE as (select user_id,
Extract(week from occurred_at) as signup_week
from events
where event_type = 'signup_flow'
and event_name = 'complete_signup' and extract(week from occurred_at)


#  calculate the weekly engagement per device.

CREATE VIEW weekly_devices AS (select
device AS devices,
extract(week from occurred_at) AS week_of_year,
dense_rank() OVER (ORDER BY extract(week from occurred_at) )  AS week_number,
count(user_id) AS user_count from events 
where event_type = 'engagement'
group by devices, week_of_year 
order by devices);

SELECT device,
ROUND(AVG(weekly_devices.user_count), 2) AS avg_weekly_device_eng
 FROM weekly_devices 
 GROUP BY device 
 ORDER BY avg_weekly_device_eng DESC;
 
# calculate the email engagement metrics.

create view Q3 as 
(WITH WeeklyUserActions AS (SELECT action,
        TIMESTAMPDIFF(WEEK , '2013-01-01 04:40:10', occurred_at) AS wk,
        COUNT(user_id) AS user_count
    FROM email_events
    GROUP BY action, wk)
SELECT action,
    ROUND(AVG(user_count), 2) AS Avg_Week_Email_Eng
FROM WeeklyUserActions
GROUP BY action
ORDER BY Avg_Week_Email_Eng DESC);

select * from Q3;
select count(distinct user_id) as total_users from users;

# total users recieving mails and total mails received
select count(distinct user_id) as users_receiving_mails_cnt,
round(count(distinct user_id)/9381, 2) * 100 as perc_users_with_email_activity,
count(user_id) as total_emails
from email_events;

# percentage of users with email_actions
select action,
Avg_Week_Email_Eng,
round(Avg_Week_Email_Eng/90389 * 100, 2) as perc_of_users
from Q3;

# users with no email activity

select users.user_id, company_id, 
language from users  
left join email_events 
on users.user_id = email_events.user_id 
where email_events.user_id is NULL
order by user_id asc;

select user_id,
count(action) as email_type_peruser_count
from email_events
where action = 'sent_weekly_digest' 
group by user_id;