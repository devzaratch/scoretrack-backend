package main

// Phase 1d — Football news (BBC Sport RSS, free, no key, no quota).
// Backend fetches the feed at most once per hour, caches in memory,
// and serves GET /api/news. Approved R-NEWS 5-Oct-2026.

import (
	"encoding/xml"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const newsFeedURL = "https://feeds.bbci.co.uk/sport/football/rss.xml"
const newsCacheTTL = time.Hour

type rssThumbnail struct {
	URL string `xml:"url,attr"`
}

type rssItem struct {
	Title       string       `xml:"title"`
	Description string       `xml:"description"`
	Link        string       `xml:"link"`
	PubDate     string       `xml:"pubDate"`
	GUID        string       `xml:"guid"`
	Thumbnail   rssThumbnail `xml:"thumbnail"`
}

type rssChannel struct {
	Title string    `xml:"title"`
	Items []rssItem `xml:"item"`
}

type rssFeed struct {
	Channel rssChannel `xml:"channel"`
}

type newsArticle struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Link        string `json:"link"`
	PubDate     string `json:"pubDate"`
	Image       string `json:"image"`
}

type newsCache struct {
	mu      sync.RWMutex
	at      time.Time
	articles []newsArticle
}

var footballNews = &newsCache{}

var newsHTTP = &http.Client{Timeout: 15 * time.Second}

func fetchFootballNews() []newsArticle {
	footballNews.mu.RLock()
	if len(footballNews.articles) > 0 && time.Since(footballNews.at) < newsCacheTTL {
		cached := footballNews.articles
		footballNews.mu.RUnlock()
		return cached
	}
	footballNews.mu.RUnlock()

	req, err := http.NewRequest("GET", newsFeedURL, nil)
	if err != nil {
		return staleNews()
	}
	req.Header.Set("User-Agent", "ScoreTrack/1.0 (+livescore)")
	resp, err := newsHTTP.Do(req)
	if err != nil {
		log.Printf("news fetch failed: %v (serving stale)", err)
		return staleNews()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("news fetch status %d (serving stale)", resp.StatusCode)
		return staleNews()
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return staleNews()
	}
	var feed rssFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		log.Printf("news parse failed: %v", err)
		return staleNews()
	}
	articles := make([]newsArticle, 0, len(feed.Channel.Items))
	for _, it := range feed.Channel.Items {
		title := strings.TrimSpace(it.Title)
		if title == "" {
			continue
		}
		articles = append(articles, newsArticle{
			Title:       title,
			Description: strings.TrimSpace(it.Description),
			Link:        strings.TrimSpace(it.Link),
			PubDate:     strings.TrimSpace(it.PubDate),
			Image:       strings.TrimSpace(it.Thumbnail.URL),
		})
	}
	if len(articles) == 0 {
		return staleNews()
	}
	footballNews.mu.Lock()
	footballNews.articles = articles
	footballNews.at = time.Now()
	footballNews.mu.Unlock()
	return articles
}

func staleNews() []newsArticle {
	footballNews.mu.RLock()
	defer footballNews.mu.RUnlock()
	if len(footballNews.articles) == 0 {
		return []newsArticle{}
	}
	return footballNews.articles
}

func newsHandler(c *gin.Context) {
	articles := fetchFootballNews()
	c.Header("X-Data-Source", "bbc-rss")
	c.JSON(http.StatusOK, gin.H{
		"source":  "BBC Sport",
		"count":   len(articles),
		"items":   articles,
	})
}
