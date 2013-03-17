var sys = require("sys");
var request = require('./node-v0.8.18-linux-x86/bin/node_modules/request');
http = require("http");
qs = require("querystring");
PORT = 31365;
url = require("url");
var cronJob = require('./node-v0.8.18-linux-x86/bin/node_modules/cron').CronJob;

MIME_TYPES ={
	'.html': 'text/html',
	'.js': 	'text/javascript',
	'.txt': 'text/plain'
};

//handling tumblr
var KEY = 'VdAQkUPDY46fUmqRVGqRCY3ncJvrx6SDKAl5bQN7Tw2xZgxeY9';
//timeout
// Handling Database
var _mysql = require("./node-v0.8.18-linux-x86/bin/node_modules/mysql");
var _HOST = "dbsrv1.cdf.toronto.edu";
var _PORT = "3306"; // standard sql PORT
var _USER = "g2junhee";
var _PASS = "eebiepic";
var _DATABASE = "csc309h_g2junhee"; // this database? or a2.sql we created? 
											/* we'll have to run a2.sql just 
											 * once on csc309h_{cdf_user_name} to create the tables */
// db tables
var BLOG_TBL = "blog";
var POST_TBL = "post";
var LIKES_TBL = "likes";
var TMSTMP_TBL = "time_stamp";

// mysql -p -h dbsrv1 -u g2_junhee -p eebiepic csc309h_g2junhee
var mysql = _mysql.createConnection({
	host: _HOST,
	port: _PORT,
	user: _USER,
	password: _PASS,
	database: _DATABASE
});


mysql.connect(function(error, results) {
	if(error) {
		console.log('Connection Error: ' + error.message);
		return;
	}
	console.log('Connected to database');
});

function extractData(basename, order, limit, onSuccess, onErr) {
	if (basename == "") {
		if (order == "Trending") {
			mysql.query("select P.url, P.txt, P.img, P.dt, T.ts, T.seq, T.inc, T.cnt from time_stamp T, post P where T.ts > '"+getTime(1)+"' and T.url=P.url order by inc DESC limit 0, " + limit, function (error, results) {
			if (error) {
				console.log('Select Error: ' + error.message);
				mysql.end();
				onErr();
			}
			if (results){ onSuccess(results); }
			});
		}
		else if (order == "Recent") {
			mysql.query("select * from time_stamp T, (select * from post order by dt DESC limit 0, "+limit+") P where T.url=P.url order by dt desc, seq desc" ,function (error, results) {
			if (error) {
				console.log('Select Error: ' + error.message);
				mysql.end();
				onErr();
			}
			if (results){ onSuccess(results); }
			});
		}
	}
	else if (basename) {
		if (order == "Trending") {
			mysql.query("SELECT P.url, P.txt, P.img, P.dt, T.ts, T.seq, T.inc, T.cnt "+
				    "FROM time_stamp T, (select post.url, post.txt, post.img, post.dt from post, blog, likes where likes.person=blog.url and blog.url="+basename+" likes.url=post.url) P "+
				    "WHERE T.ts > '"+getTime(1)+"' and T.url=P.url ORDER BY inc DESC LIMIT 0, " + limit ,function (error, results) {
				if (error) {
					console.log('Select Error: ' + error.message);
					mysql.end();
					onErr();
				}
				if (results){ onSuccess(results); }
			});
			
		}
		else if (order == "Recent") {
			mysql.query("select * " + 
				    "from time_stamp T, (select P.url, P.txt, P.img, P.dt from blog B, post P, likes L where L.person=B.url and L.url=P.url and B.url='"+basename +"' order by dt DESC limit 0, "+limit+") D " +
				    "where T.url=D.url order by dt desc, seq desc" ,function (error, results) {
				if (error) {
					console.log('Select Error: ' + error.message);
					mysql.end();
					onErr();
				}
				if (results){ onSuccess(results); }
			});
		  
		}
	}
}

function insertDB(tbl, data, hostname, onSuccess, onErr) {
	if (tbl == BLOG_TBL) {
				mysql.query("insert into blog values ('"+data+"')", function (err, results, fields) {
					if (err) {
						console.log('Insert Error: ' + error.message);
						mysql.end();
					}
				});
	}
	else if (tbl == POST_TBL) {
		existsInDB(tbl, "url", data.post_url, "", function (exists) {
			if (!exists) {
				var cols = "url, txt, img, dt";
				var img = "";
				if (data.image_permalink) {
				img = data.image_permalink;
				}
				var vals = data.post_url + "', "
				+ "'" + data.slug + "', "
				+ "'" + img + "', "
				+ "'" + data.date;
				mysql.query("insert into " + tbl + " (" + cols + ") values ('" + vals + "')", function (err, results, fields) {
					if (err) {
						console.log('Insert Error: ' + error.message);
						mysql.end();
					}
				});
				mysql.query("insert into " + LIKES_TBL + " values ('"+data.post_url+"', '"+hostname+"')", function(err, results, fields) {
					if (err) {
						console.log('Insert Error: ' + error.message);
						mysql.end();
					}
				});
				onSuccess(0, data, hostname);
			} else {
				existsInDB(LIKES_TBL, "url", data.post_url+"' and person='"+hostname, "", function (exists) {
					if (!exists) {
						mysql.query("insert into " + LIKES_TBL + " values ('"+data.post_url+"', '"+hostname+"')", function(err, results, fields) {
							if (err) {
								console.log('Insert Error: ' + error.message);
								mysql.end();
							}
						});
					}
				});
				onSuccess(1, data, hostname);
			}
		});
	}
	else if (tbl == TMSTMP_TBL) {
		existsInDB(tbl, "url", data.post_url, "", function (exists) {
			if (!exists) {
				var cols = "ts, url, seq, inc, cnt";
				var vals = getTime() + "', '"
					 + data.post_url + "', '"
					 + "0', '0', '" + data.note_count;
				mysql.query("insert into " + tbl + " (" + cols +") values ('" + vals + "')", function (err, results, fields) {
					if (err) {
						console.log('Insert Error: ' + error.message);
						mysql.end();
					}
				});
			}
			else {
				var cols = "ts, url, seq, inc, cnt";
				var vals = getTime() + "', '"
					 + data.post_url + "', '" + data.seq + "', '" + data.inc + "', '" + data.note_count;
 				mysql.query("insert into " + tbl + " (" + cols +") values ('" + vals + "')", function (err, results, fields) {
					if (err) {
						console.log('Insert Error: ' + error.message);
						mysql.end();
					}
				});
			}
		});
	}
}
// 		posts = database("GET", POST_TBL, "", "*", "");
function existsInDB(tbl, field, value, key, onSuccess, onErr) {
	mysql.query("select exists(select * from " + tbl + " where " + field + " = '" + value + "') exist", function (error, results, fields) {
		if (error) {
			console.log('Exists	 Error: ' + error.message);
			mysql.end();
			onErr();
		}
		if (results){ onSuccess(results[0].exist, key); }
	});
	
}


/*************************** POST METHOD FUNCTIONS ***************************/

/*
 * Insert a blog URL into database
 */
function insertBlog(hostname) {
	// Url of a blogger that we are tracking.
	insertDB(BLOG_TBL, hostname);
}

/*
 * Returns the current time in this format Last Track: 2013-03-11 23:45:57
 */
function getTime(lastHour){
	var currentdate = new Date();
	if (lastHour) {
		currentdate.setHours(currentdate.getHours() - 1);
		currentdate.setMinutes(currentdate.getMinutes() - 10);
	}
	var datetime = currentdate.getFullYear() + "-" + checknumber(currentdate.getMonth()+1) + "-" + checknumber(currentdate.getDate()) + " "
		      + currentdate.getHours() + ":"  
		      + currentdate.getMinutes() + ":" 
		      + currentdate.getSeconds() + "EST";
	return datetime;
}

/*
 * Adds an 0 to the number if it is smaller than 10
 */
function checknumber(time){
	 return (time < 10) ? ("0" + time) : time;  
}

function insertLikesHelper(hostname, count) {
	var off;
	for (off = 0; off < count+50; off+=50) {
		request.get({url:'http://api.tumblr.com/v2/blog/'+hostname+'/likes?api_key='+KEY+'&limit=50&offset='+off, json:true}, function (error, response, body) {
			if (!error) {
				var post;
				var vals;
				var pcols = "url, blog_url, txt, img, dt";
				var tcols = "ts, url, seq, inc, cnt";
				for (var i=0; i<50; i++) {
					post = body.response.liked_posts[i];
					if (post) {
						insertDB(POST_TBL, post, hostname, function(exists, p, hn) {
							if(!exists) {
								insertDB(TMSTMP_TBL, p, hn);
							}
						});
					}
				}
			}
		}); 
	}
}
/*
 * Insert into db info about liked posts of a blog specified by 'hostname'
 * TODO: figure out what is 'text' and how to handle images
 */
function insertLikes(hostname) {
	request.get({url:'http://api.tumblr.com/v2/blog/'+hostname+'/likes?api_key='+KEY+'&limit=51', json:true}, function (error, response, body) {
		if (!error) {
			insertLikesHelper(hostname, body.response.liked_count);
		}
		else{console.log("error in insertlikes"); }
	}) 
}
/* helper of updateDB, finds all hostname
 */
function gethosts(onSuccess, onErr) {
		mysql.query("select url from blog", function (error, results) {
		if (error) {
		  console.log('Select hosts Error: ' + error.message);
		  mysql.end();
		  onErr();
		}
		if(results) { onSuccess(results); }
		else {onSuccess(0); }
	});
}
/*
 * updateTracking helper. Get the largest seq value for post
 */
function getMaxSeq(post_url, onSuccess, onErr) {
	mysql.query("select max(seq) as max_seq from time_stamp where url = '"+post_url+"'", function (error, results, fields) {
		if (error) {
		  console.log('Select MAX Error: ' + error.message);
		  mysql.end();
		  onErr();
		}
		onSuccess(results[0].max_seq);
	});
}

/*
 * updateTracking helper. Get the note_count value
 */
function getInc(post_url, seq, onSuccess, onErr) {
		mysql.query("select cnt from time_stamp where url = '"+post_url+"' and seq = '"+seq+"'", function (error, results, fields) {
		if (error) {
		  console.log('Select INC Error: ' + error.message);
		  mysql.end();
		  onErr();
		}
		onSuccess(results[0].cnt);
	});
}

/*
 * Update time_stamp table
 */
function updateTracking(data) {
	var seq = 0;
	var inc = 0;
	var post = {"ts":"", "post_url":"", "seq":0, "inc":0, "note_count":0};
	post.post_url = data.post_url;
	getMaxSeq(data.post_url, 
		function (sequence) {
			post.seq = sequence + 1;
			getInc(data.post_url, post.seq-1, function(note_count) {
				if (seq-1 != 0) {
					post.inc = data.note_count - note_count;
					post.note_count = data.note_count;
				} else {
					post.inc = 0;
				}
// 				console.log(post);
				insertDB(TMSTMP_TBL, post, "");
			}, function(err) {
				console.log('getInc Error: ' + error.message);
			});
	}, function(err) {console.log('getMaxSeq Error: ' + error.message);});
}

/*
	url varchar(50) primary key,
	blog_url varchar(50) not null,
	txt varchar(50),
	img varchar(50),
	dt timestamp not null,
	last_track date not null,
	note_count integer not null*/
/*************************** GET METHODS FUNCTIONS ***************************/
/*** GET /blog/{base-hostname}/trends is method_type 1 ***/
/*** GET /blog/trends is method_type 2 ***/

/*
 * Return post information in JSON format
 */
function trendJSON(data, order, limit, onSuccess) {
	var trends = {"order" : order, "limit" : limit, "trending": []};
	trends.order = order;
	trends.limit = limit;
	var tempTrend = [];
	var i = 0;
	while (i < data.length) {
		var trend = { "url": "", "text": "", "image": "", "date": "", "last_track": "", "last_count": 0, "tracking": []};
		if (data[i].seq > 0) {
			post = data[i];
			trend.url = post.url;
			trend.text = post.txt;
			trend.image = post.img;
			trend.date = post.dt;
			trend.last_track = post.ts;
			trend.last_count = post.cnt;
			var j = post.seq;
			while (j > 0) {
				track = {"timestamp": "", "sequence": 0, "increment": 0, "count": 0};
				post = data[i];
				track.timestamp = post.ts;
				track.sequence = post.seq;
				track.increment = post.inc;
				track.count = post.cnt;
				trend.tracking.push(track);
				i++;
				j--;
			}
			tempTrend.push(trend);
			i++;
		} else {
			post = data[i];
			trend.url = post.url;
			trend.text = post.txt;
			trend.image = post.img;
			trend.date = post.dt;
			trend.last_track = post.ts;
			trend.last_count = post.cnt;
			tempTrend.push(trend);
			i++;
		}
	}
	trends.trending = tempTrend;
	onSuccess(trends);
}

/*
* Return trend information specified by a basename in JSON format
*/
function getTrendInfo(basename, order, limit, method_type , onSuccess) {
	if (method_type == 1) { // method is GET /blog/{base-hostname}/trends
		// get post urls that are related to a specific basename (blog)
// 		//database("GET", POST_TBL, "blog_url", "*", basename);

		extractData(basename, order, limit, 
			function(posts) {
				trendJSON(posts, order, limit, function(json) {
					onSuccess(json);
				});
			}, function() { console.log("ERROR!"); });
	} else { // method is GET /blog/trends
		// get all posts that exist in the database
		extractData(basename, order, limit, 
			function(posts) { 
				trendJSON(posts, order, limit, function(json) {
					onSuccess(json);
				});
			}, function() { console.log("ERROR!"); });
	}
}

function deleteDB(tbl, url, person, onSuccess) {
	if (tbl == LIKES_TBL) {
		mysql.query("delete from "+tbl+" where url='"+url+"' and person='"+person+"'", function(err, results, fields) {
			if (err) {
				console.log("Error on deleting from time_stamp");
				mysql.end();
			}
			onSuccess(url);
		});
	}
	else {
		mysql.query("delete from "+tbl+" where url='"+url+"'", function(err, results, fields) {
			if (err) {
				console.log("Error on deleting from time_stamp");
				mysql.end();
			}
			onSuccess(url);
		});
	}
}

function checkEmptyLikes(url, onSuccess) {
	mysql.query("select * from likes where likes.url='"+url+"'", function (err, results, fields) {
		if (err) {
			console.log("Error while checking likes is empty with url");
			mysql.end();
		}
		if (results) {
			onSuccess(url);
		}
	});
}

function deleteUnlike() {
	mysql.query("select T1.url, L.person, T1.ts from time_stamp T1, likes L where T1.url=L.url and T1.seq >= (select max(T2.seq) from time_stamp T2 where T1.url=T2.url) and T1.ts < '"+getTime(1)+"'", function (err, results, fields) {
		if (err) {
			console.log("ERROR!"); 
			mysql.end();
		}
		if (results) {
			for (var i = 0; i < results.length; i++ ) {
				deleteDB(LIKES_TBL, results[i].url, results[i].person, function(lurl) {
					deleteDB(TMSTMP_TBL, lurl, "", function (turl) {
						checkEmptyLikes(turl, function (purl) {
							if (purl) {
								deleteDB(POST_TBL, purl, "", function(i) {console.log("deletion complete");});
							}
						});
					});
				});
			}
		}
	});
}

function updateDBHelper(hostname, count, onSuccess) {
	var off;
	for (off = 0; off < count+50; off+=50) {
		request.get({url:'http://api.tumblr.com/v2/blog/'+hostname+'/likes?api_key='+KEY+'&limit=50&offset='+off, json:true}, function (error, response, body) {
			if (!error) {
				var post;
				// update time_stamp table of existing blogs
				for (var i=0; i < 50; i++) {
					post = body.response.liked_posts[i];
					if (post) {
						existsInDB(TMSTMP_TBL, "url", post.post_url, post, function(exists, data){
							if (exists) {
								updateTracking(data);
							}
						});
					}
				}
			}
		}); 
	}
	onSuccess(1);
}

function getFromAPI(hostname, onSuccess){
	
	request.get({url:'http://api.tumblr.com/v2/blog/'+hostname+'/likes?api_key='+KEY+'&limit=51', json:true}, function (error, response, body) {
		if (!error) {
			updateDBHelper(hostname, body.response.liked_count, 
				function (good) {
					onSuccess(hostname);
			        });
		}
		else{console.log("error in insertlikes"); }
	});
}

function updateLike(host) {
	if (host) {
		for (var i = 0; i < host.length; i++) {
		      getFromAPI(host[i].url, function(hostname){
			      if (hostname) { insertLikes(hostname); }
		      });
		}
	} else { console.log("We are not tracking any blogger"); }
}
function updateDB(){
	//cron to count to 1 hour
	
	//if new like in read from API -> update DB 
	//if like in DB not in API -> delete column
	
	//what if someone deletes a post? not a problem cuz API will know
	//post a new post? not a problem
	//new blog created? not a problem
	gethosts(function(host){
		updateLike(host);
	}, function(err) {console.log('gethosts Error: ' + error.message);});
}

/*************************** SERVER THAT WILL HANDLE EACH EVENT ***************************/

var job = new cronJob({
	cronTime: '0 * * * *', //minute hour day month day-of-week
	onTick: function() {
		console.log("update");
		updateDB();
		setTimeout(function() {console.log("delete");deleteUnlike();}, 300000);
	},
	start: true, //or use job.start() outside
});

http.createServer(function(req, res) {

	console.log(req.url);
	if (req.url == '/') {
		var msg = "This is CSC309H1 winter 2013 class Assignment 2";
		res.writeHead(200, {
 				  'Content-Type': 'application/json', 
				  'Access-Control-Allow-Origin': '*'
 				});
		res.write(msg);
		res.end();
	}
	if (req.method == 'POST') {
		// parameter: blog
		//            a string indicating a new blog to track by its {base-hostname}
		if (req.url == '/blog') {
			
			var rawData = "";
			
			// Load {base-hostname}.
			req.on('data', function(buf){
				rawData += buf.toString();
			});
			
			req.on('end', function() {
				var hostname = qs.parse(rawData).blog;
				existsInDB(BLOG_TBL, "url", hostname, "", function (exists) {
					if (!exists) {
						insertBlog(hostname); // tracking blogs
						insertLikes(hostname); // post liked by our tracking blogs.
						// RESPONSE: HTTP status 200 if accepted.
						res.writeHead(200);
						res.end();
					} else {
						console.log("This host is already being tracked");
						res.wirteHead(200);
						res.end();
					}
				});
			});
		}
	} 
	if (req.method == 'GET') {
		if(req.url == '/update'){
			updateDB();
			res.writeHead(200);
			res.end();
		} else if (req.url == '/delete'){
			deleteUnlike();
			res.writeHead(200);
			res.end();
		}
		// parameter: order as 1st argument of -d in curl
		//	      "Trending" or "Recent" indicating how to order JSON
		//	      "Trending" - posts that have the largest increments in note_count in the last hour
		//            "Recent" - most recent posts regardless of  their popularity.
		// parameter: limit (optional) as 2nd argument of -d in curl
		//	      the maximum number of results to return.
		// RESPONSE: JSON including trend, or recent info
		var url_parts = url.parse(req.url,true);
		var pathname = url_parts.pathname;
		var param = url_parts.query;
		if (pathname == '/blogs/trends') {
			var limit = 20;
			order = param.order; // order is always presented
			if (param.limit) {
			      limit = param.limit; // find limit if exists
			}
			getTrendInfo("", order, limit, 2, function(trendinfo) {
 				res.writeHead(200, {
 				  'Content-Type': 'application/json', 
 				  'Access-Control-Allow-Origin': '*'
 				});
				res.write(JSON.stringify(trendinfo));
				res.end();
			});
		}
		//return the ordered posts of the specified hostname's likes
		// _ / blog / hostname/ trends
		// 0     1        2        3
		else if (pathname.split("/").length == 4) {
			s = pathname.split("/");
			var bt = s[1]+"/"+s[3];
			if (bt == "blog/trends") {
				var hostname = s[2];
				var limit = 20;
				order = param.order; // order is always presented
				if (param.limit) {
				      limit = param.limit; // find limit if exists
				}
				existsInDB(BLOG_TBL, "url", hostname, "", function(exist) {
					getTrendInfo(hostname, order, limit, 1, function(trendinfo) {
						res.writeHead(200, {
						  'Content-Type': 'application/json', 
						  'Access-Control-Allow-Origin': '*'
						});
						res.write(JSON.stringify(trendinfo));
						res.end();
						
					});
				});
			}
		}
		else {
			res.writeHead(404);
			res.end();
		}
	}
}).listen(PORT);

console.log('Server running at http://greywolf.cdf.toronto.edu:' + PORT +'/');
