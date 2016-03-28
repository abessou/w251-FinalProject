
// Run with command:
// mongo code/mongo_scripts/get_top_videos.js

// Connect to database
var conn = new Mongo();
db = conn.getDB("VideosDB");

var N =10;

// === Facebook ===
// Get Top Facebook Videos by Number of Likes
db.getCollection('top_fb_ByLikes').drop()
db.getCollection('facebook').aggregate([
    { $sort: {total_likes : -1} },
	{ $limit: N },
    {$out : 'top_fb_ByLikes'}
])

// Get Top Facebook Videos by Number of Comments
db.getCollection('top_fb_ByComments').drop()
db.getCollection('facebook').aggregate([
    { $sort: {total_comments : -1} },
	{ $limit: N },
    {$out : 'top_fb_ByComments'}
])

// === Twitter ===
// Get Top Twitter Videos by Number of Retweets
db.getCollection('top_tw_ByRetweets').drop()
db.getCollection('twitter').aggregate([
    { $sort: {'tweet.orig_retweet_count' : -1} },
	{ $limit: N },
    {$out : 'top_tw_ByRetweets'}
])

// Get Top Twitter Videos by Favorite Count
db.getCollection('top_tw_ByFavorite').drop()
db.getCollection('twitter').aggregate([
    { $sort: {'tweet.tweet.orig_favorite_count' : -1} },
	{ $limit: N },
    {$out : 'top_tw_ByFavorite'}
])

// === YouTube ===
// Get Top YouTube Videos by Number of Views
db.getCollection('top_yt_ByViews').drop()
db.getCollection('Youtube').aggregate([
    { $sort: {'statistics.viewCount' : -1} },
	{ $limit: N },
    {$out : 'top_yt_ByViews'}
])

// Get Top YouTube Videos by Favorite Count
db.getCollection('top_yt_ByFavorite').drop()
db.getCollection('Youtube').aggregate([
    { $sort: {'statistics.favoriteCount' : -1} },
	{ $limit: N },
    {$out : 'top_yt_ByFavorite'}
])

// Get Top YouTube Videos by Number of Comments
db.getCollection('top_yt_ByComments').drop()
db.getCollection('Youtube').aggregate([
    { $sort: {'statistics.commentCount' : -1} },
	{ $limit: N },
    {$out : 'top_yt_ByComments'}
])


