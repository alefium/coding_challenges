
/*
CREATE SCHEMA dna_test;
CREATE TABLE dna_test.flights
(
	flight_nr INT,
	start_loc VARCHAR(255),
	end_loc VARCHAR(255),
	depart_time TIME
);

CREATE TABLE dna_test.customer_arrival
(
	customer_id VARCHAR(255),
	arrived_at VARCHAR(255),
	depart_to VARCHAR(255),
	arrival_time TIME
);
INSERT INTO dna_test.flights 
       VALUES (101,'Berlin','Paris','03:30'),
		   (102,'Berlin','Paris','05:00'),
		   (103,'Paris','New York','06:00'),
		   (104,'London','New Delhi','07:00'),
		   (105,'Berlin','Paris','19:00'),
		   (106,'Paris','New York','20:00');
									
INSERT INTO dna_test.customer_arrival 
       VALUES ('X','Berlin','Paris','02:40'),
		   ('Y','London','New Delhi','01:00'),
		   ('Z','Paris','New York','19:30'),
		   ('A','London','New Delhi','07:00'),
		   ('B','Berlin','Paris','16:00'),
		   ('C','Paris','New York','13:00');
*/


select aa.flight_nr
	,count(customer_id) as nr_customers
from dna_test.flights aa
-- Getting the time of the previous flight with the same 
-- start and end location, if it exists.
left join lateral (
	select bbb.depart_time as prev_depart_time
	from dna_test.flights bbb
	where bbb.start_loc = aa.start_loc
		and bbb.end_loc = aa.end_loc
		and bbb.depart_time < aa.depart_time
	order by bbb.depart_time desc
	limit 1
			 ) bb
	on true
-- Assigning each customer to his flight.
left join dna_test.customer_arrival cc
	on cc.arrived_at = aa.start_loc
	and cc.depart_to = aa.end_loc
	and cc.arrival_time > coalesce(bb.prev_depart_time, '00:00:00') 
	and cc.arrival_time <= aa.depart_time
group by aa.flight_nr
order by aa.flight_nr
;












































