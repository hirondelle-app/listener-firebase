package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/ChimeraCoder/anaconda"
	"github.com/jessevdk/go-flags"
	"github.com/zabawaba99/firego"
)

const (
	firebaseURL = "https://hirondelle-e44d5.firebaseio.com/"
)

var (
	keywords       []string
	tweetsInserted []string
	firebase       *firego.Firebase

	config struct {
		Likes    int    `short:"l" long:"likes" description:"Minimun likes for a tweet" required:"true"`
		Retweets int    `short:"r" long:"retweets" description:"Minimun retweets for a tweet" required:"true"`
		Token    string `short:"t" long:"token" description:"Firebase token" required:"true"`
		Twitter  struct {
			AccessToken       string `long:"access-token" description:"Access token for Twitter Api" required:"true"`
			AccessTokenSecret string `long:"access-token-secret" description:"Secret access token for Twitter Api" required:"true"`
			ConsumerKey       string `long:"consumer-key" description:"Consumer key for Twitter Api" required:"true"`
			ConsumerSecret    string `long:"consumer-secret" description:"Consumer secret for Twitter Api" required:"true"`
		}
	}
)

func main() {

	initLog()

	log.Info("Starting listener")

	_, err := flags.Parse(&config)

	if err != nil {
		log.Fatal(err)
	}

	anaconda.SetConsumerKey(config.Twitter.ConsumerKey)
	anaconda.SetConsumerSecret(config.Twitter.ConsumerSecret)
	api := anaconda.NewTwitterApi(config.Twitter.AccessToken, config.Twitter.AccessTokenSecret)

	firebase = firego.New(firebaseURL, nil)
	firebase.Auth(config.Token)

	notifications := make(chan firego.Event)
	fbRealTime := firebase.Child("keywords/byId")

	if err := fbRealTime.Watch(notifications); err != nil {
		log.Fatal(err)
	}

	defer fbRealTime.StopWatching()

	for _ = range notifications {

		log.Info("Watching keywords")

		keywords = getKeywords()
		log.Info(keywords)

		keywordsStr := keywordsToStr()
		log.Infof(keywordsStr)

		v := url.Values{}
		v.Set("track", keywordsStr)
		v.Set("languages", "en")
		s := api.PublicStreamFilter(v)

		defer s.Stop()

		quit := make(chan bool)

		go streamTweets(s, quit)
	}
}

func streamTweets(s *anaconda.Stream, quit chan bool) {

	log.Info("Streaming tweets")

	for {
		item := <-s.C
		switch status := item.(type) {
		case anaconda.Tweet:
			if retweet := status.RetweetedStatus; retweet != nil {
				if retweet.RetweetCount >= config.Retweets && retweet.FavoriteCount >= config.Likes {
					if isTweetInserted := stringInSlice(retweet.IdStr, tweetsInserted); isTweetInserted == false {

						keyword := getKeywordFromTweet(retweet)

						if keyword == "" {
							return
						}

						fbTweets := firebase.Child(fmt.Sprintf("tweets/byId/%s", retweet.IdStr))

						t := map[string]interface{}{
							"tweetId":   retweet.IdStr,
							"likes":     retweet.FavoriteCount,
							"retweets":  retweet.RetweetCount,
							"createdAt": retweet.CreatedAt,
						}

						if err := fbTweets.Set(t); err != nil {
							log.Fatal(err)
						}

						tweetKeyword := map[string]string{retweet.IdStr: "true"}

						fbTweetKeywords := firebase.Child(fmt.Sprintf("tweets/byKeyword/%s", keyword))
						if err := fbTweetKeywords.Update(tweetKeyword); err != nil {
							log.Fatal(err)
						}

						log.Infof("%+v\n", t)

						tweetsInserted = append(tweetsInserted, retweet.IdStr)
					}
				}
			}
		}
	}
}

func getKeywordFromTweet(tweet *anaconda.Tweet) string {

	var keyword string

	// Get hashtag
	for _, k := range keywords {
		for _, h := range tweet.Entities.Hashtags {
			hashtagLower := strings.ToLower(h.Text)
			if k == hashtagLower {
				keyword = k
				return keyword
			}
		}
	}

	// We still don't have a keyword, so we take user @mention
	for _, k := range keywords {
		for _, u := range tweet.Entities.User_mentions {
			userLower := strings.ToLower(u.Screen_name)
			if k == userLower {
				keyword = k
				return keyword
			}
		}
	}

	return keyword
}

func getKeywords() []string {
	var keywords []string

	fbKeywords := firebase.Child("keywords/byId")

	var results map[string]interface{}
	if err := fbKeywords.Value(&results); err != nil {
		log.Fatal(err)
	}

	for k := range results {
		keywords = append(keywords, k)
	}

	return keywords
}

func keywordsToStr() string {
	var customKeywords string

	for _, k := range keywords {
		customKeywords += fmt.Sprintf("@%[1]s,#%[1]s,", k)
	}

	return customKeywords

}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func initLog() {
	customFormatter := new(log.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.ForceColors = true
	log.SetFormatter(customFormatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

}
