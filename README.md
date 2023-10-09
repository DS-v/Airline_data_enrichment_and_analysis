# Airline_data_enrichment_and_analysis

## Problem Statement:

A daily file having data on airplane flights is forwarded by upstream team by uploading it to a S3 bucket. Data on airports between which these flights takes place is already present on the database. An automated system needs to be developed which enriches the flight data with data on airports, filters the records based on business logic and stores it on an OLAP style database in a de-normalized form. Furthermore, the data needs to be analysed.

Business logic dictates that data on flights whose departure has been delayed by more than an hour are classified as "DELAYED", those only needed to be stored.

## Design:

![airline_data_enrichment drawio (1)](https://github.com/DS-v/Airline_data_enrichment_and_analysis/assets/59478620/fdbba642-63f0-44b2-ba6e-357de2af22bb)

The execution graph for the Step function is as follows:

![stepfunctions_graph](https://github.com/DS-v/Airline_data_enrichment_and_analysis/assets/59478620/68b34317-c73d-4921-84ba-bb37fce48d5e)

Here choice 1 is based on whether crawler is running or run is successfull. Choice 2 is based on whether Glue job succeded or not.

## Analysis of dataset:

=> The enriched dataset takes the following form - 

carrier	dep_airport	arr_airport	dep_city	arr_city	dep_state	arr_state	dep_delay	arr_delay
WN	McCarran International	Lambert-St. Louis International	Las Vegas	St. Louis	NV	MO	62	51	
WN	McCarran International	Lambert-St. Louis International	Las Vegas	St. Louis	NV	MO	117	120	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	92	94	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	62	74	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	125	136	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	263	268	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	285	291	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	93	93	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	76	70	
AA	Chicago O'Hare International	Lambert-St. Louis International	Chicago	St. Louis	IL	MO	74	73	

=> Out of 100,000 records, 66,863 records were filtered to be in the dataset - 

count
66863	

=> Top 10 airplane routes with most number of delayed flights and the flight count

dep_airport	arr_airport	count
Los Angeles International	San Francisco International	463	
San Francisco International	Los Angeles International	365	
Chicago O'Hare International	LaGuardia	353	
Chicago O'Hare International	San Francisco International	290	
LaGuardia	Chicago O'Hare International	283	
Dallas/Fort Worth International	Chicago O'Hare International	272	
Chicago O'Hare International	Newark Liberty International	263	
Chicago O'Hare International	Los Angeles International	235	
Chicago O'Hare International	Hartsfield-Jackson Atlanta International	226	
McCarran International	San Francisco International	224	

=> Top 10 airpolane traffic routes with most number of delayed flights in terms of city

dep_city	arr_city	count
Chicago	New York	501	
Los Angeles	San Francisco	463	
New York	Chicago	444	
San Francisco	Los Angeles	365	
Chicago	San Francisco	308	
Chicago	Newark	304	
Washington	New York	290	
Washington	Chicago	285	
Chicago	Atlanta	279	
Dallas/Fort Worth	Chicago	272	

=> Top airports arranged in decreasing order of departure delay

dep_airport	avg	stddev
Honolulu International	147	102.35888934980137	
Port Columbus International	144	110.19988939368726	
Indianapolis International	139	117.00104288469288	
Memphis International	138	99.82845632849762	
Ronald Reagan Washington National	137	106.12706400397717	
Will Rogers World	137	103.02437485627499	
Tucson International	136	119.90512378408779	
Southwest Florida International	136	102.59029392541022	
Buffalo Niagara International	136	96.76686325490726	
Norfolk International	135	104.86653969686645	

=> Top airports arranged in decreasing order of arrival delay

arr_airport	avg	stddev
Honolulu International	148	133.70811452818387	
Chicago O'Hare International	140	107.86482010881545	
Dallas/Fort Worth International	137	132.1862403075537	
Hartsfield-Jackson Atlanta International	135	102.2559822954357	
Miami International	133	122.57234883348954	
Kahului Airport	132	76.52661245885432	
Detroit Metro Wayne County	128	89.35894672945108	
Minneapolis-St Paul International	127	90.92004955514575	
George Bush Intercontinental/Houston	126	80.78182227794103	
John F. Kennedy International	125	77.10202587493136	

=> Carriers which are most used

carrier	count
AA	11084	
UA	10104	
WN	7883	
DL	5648	
MQ	5529	
EV	4904	
B6	4753	
US	4531	
OO	4235	
9E	3040	

=> Carriers causing most departure delay

carrier	avg
HA	189	
DL	134	
AA	129	
MQ	128	
F9	126	
VX	126	
UA	124	
9E	124	
B6	121	
EV	120	

=> Carriers causing most arrival delay

carrier	avg
HA	183	
DL	131	
MQ	130	
F9	130	
AA	126	
VX	125	
B6	121	
UA	119	
YV	119	
9E	119	











