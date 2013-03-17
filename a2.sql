-- blogs we are tracking
create table blog (
	url varchar(500) primary key
);

-- posts we are tracking
create table post (
	url varchar(500) primary key,
	txt varchar(500),
	img varchar(500),
	dt timestamp not null
);


-- time stamp associated with posts
create table time_stamp (
	id integer auto_increment primary key, 
	ts timestamp,
	url varchar(500) references post(url),
	seq integer,
	inc integer,
	cnt integer
);

create table likes (
	url varchar(500) references post(url),
	person varchar(500) references blog(url),
	primary key(url, person)
);