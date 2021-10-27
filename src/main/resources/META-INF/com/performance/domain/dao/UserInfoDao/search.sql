select
   id
  ,last_name
  ,first_name
  ,prefectures
  ,city
  ,blood_type
  ,hobby1
  ,hobby2
  ,hobby3
  ,hobby4
  ,hobby5
from user_info
where
  prefectures = /*entity.prefectures*/'1'
and
  city = /*entity.city*/'1'
and
  blood_type = /*entity.bloodType*/'1'
and
  hobby1 IN /*hobbies*/('1','2')
and
  hobby2 IN /*hobbies*/('1','2')
and
  hobby3 IN /*hobbies*/('1','2')
and
  hobby4 IN /*hobbies*/('1','2')
and
  hobby5 IN /*hobbies*/('1','2')