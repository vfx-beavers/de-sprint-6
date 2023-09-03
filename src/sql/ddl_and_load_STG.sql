DROP TABLE IF EXISTS STV2023081241__STAGING.group_log;
DROP TABLE IF EXISTS STV2023081241__STAGING.group_log_rej;

CREATE TABLE STV2023081241__STAGING.group_log
(
    group_id INT NOT NULL,
    user_id INT,
    user_id_from INT,
    event varchar(50),
    datetime timestamp
);

COPY STV2023081241__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
FROM LOCAL 'E:\\Data\\Lab\\S6 Аналитические базы данных\\data\\group_log.csv'
DELIMITER ','
REJECTED DATA AS TABLE STV2023081241__STAGING.group_log_rej;