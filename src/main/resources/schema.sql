create table if not exists user_info (
     id serial
    ,last_name varchar(256) not null
    ,first_name varchar(256) not null
    ,prefectures varchar(8) not null
    ,city varchar(64) not null
    ,blood_type varchar(2) not null
);

create table if not exists user_hobby (
     id integer
    ,hobby1 varchar(256)
    ,hobby2 varchar(256)
    ,hobby3 varchar(256)
    ,hobby4 varchar(256)
    ,hobby5 varchar(256)
);