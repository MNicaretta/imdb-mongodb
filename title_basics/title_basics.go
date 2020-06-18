package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TitleBasics struct {
	RuntimeMinutes *int           `json:"runtimeMinutes"`
	Genres         sql.NullString `json:"genres"`
	EndYear        *int           `json:"endYear"`
	Tconst         int            `json:"tconst"`
	PrimaryTitle   *string        `json:"primaryTitle"`
	TitleType      *string        `json:"titleType"`
	StartYear      *int           `json:"startYear"`
	OriginalTitle  *string        `json:"originalTitle"`
	IsAdult        bool           `json:"isAdult"`
}

type TitleRating struct {
	NumVote       int     `json:"numVote"`
	Tconst        int     `json:"tconst"`
	AverageRating float32 `json:"averageRating"`
}

type TitleCrew struct {
	Directors sql.NullString `json:"directors"`
	Writers   sql.NullString `json:"writers"`
	Tconst    int            `json:"tconst"`
}

type TitleEpisode struct {
	EpisodeNumber *int `json:"episodeNumber"`
	Tconst        int  `json:"tconst"`
	SeasonNumber  *int `json:"seasonNumber"`
	ParentTconst  *int `json:"parentTconst"`
}

type TitleAkas struct {
	Types           *string `json:"types"`
	IsOriginalTitle bool    `json:"isOriginalTitle"`
	Ordering        *int    `json:"ordering"`
	Tconst          int     `json:"tconst"`
	Language        *string `json:"language"`
	Region          *string `json:"region"`
	Title           *string `json:"title"`
	Attributes      *string `json:"attributes"`
}

type TitleCrewMongo struct {
	Directors []int `json:"directors"`
	Writers   []int `json:"writers"`
}

type TitleAkasMongo struct {
	Types           *string `json:"types"`
	IsOriginalTitle bool    `json:"isOriginalTitle"`
	Ordering        *int    `json:"ordering"`
	Language        *string `json:"language"`
	Region          *string `json:"region"`
	Title           *string `json:"title"`
	Attributes      *string `json:"attributes"`
}

type TitleBasicsMongo struct {
	Tconst         int               `json:"tconst"`
	RuntimeMinutes *int              `json:"runtimeMinutes"`
	Genres         []string          `json:"genres"`
	EndYear        *int              `json:"endYear"`
	PrimaryTitle   *string           `json:"primaryTitle"`
	TitleType      *string           `json:"titleType"`
	StartYear      *int              `json:"startYear"`
	OriginalTitle  *string           `json:"originalTitle"`
	IsAdult        bool              `json:"isAdult"`
	Directors      []int             `json:"directors"`
	Writers        []int             `json:"writers"`
	NumVotes       *int              `json:"numVotes"`
	AverageRating  *float32          `json:"averageRating"`
	Akas           []*TitleAkasMongo `json:"akas"`
	EpisodeNumber  *int              `json:"episodeNumber"`
	SeasonNumber   *int              `json:"seasonNumber"`
	ParentTconst   *int              `json:"parentTconst"`
}

func main() {
	fmt.Println("Importing title_basics")

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/imdb2")

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	collection := client.Database("imdb").Collection("title_basics")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()

	ratingMap := make(map[int]*TitleRating)
	crewMap := make(map[int]*TitleCrewMongo)
	episodeMap := make(map[int]*TitleEpisode)
	akasMap := make(map[int][]*TitleAkasMongo)

	fmt.Println("Parsing ratings")

	results, err := db.Query("SELECT numVotes, averageRating, tconst FROM title_ratings")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var titleRating TitleRating

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titleRating.NumVote, &titleRating.AverageRating, &titleRating.Tconst)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		ratingMap[titleRating.Tconst] = &titleRating
	}

	fmt.Println("Parsing crews")

	results, err = db.Query("SELECT directors, writers, tconst FROM title_crew")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var titleCrew TitleCrew

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titleCrew.Directors, &titleCrew.Writers, &titleCrew.Tconst)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		titleCrewMongo := new(TitleCrewMongo)

		if titleCrew.Directors.Valid {
			stringArray := strings.Split(titleCrew.Directors.String, ",")

			titleCrewMongo.Directors = make([]int, len(stringArray))

			for i := 0; i < len(stringArray); i++ {
				titleCrewMongo.Directors[i], _ = strconv.Atoi(stringArray[i])
			}
		} else {
			titleCrewMongo.Directors = make([]int, 0)
		}

		if titleCrew.Writers.Valid {
			stringArray := strings.Split(titleCrew.Writers.String, ",")

			titleCrewMongo.Writers = make([]int, len(stringArray))

			for i := 0; i < len(stringArray); i++ {
				titleCrewMongo.Writers[i], _ = strconv.Atoi(stringArray[i])
			}
		} else {
			titleCrewMongo.Writers = make([]int, 0)
		}

		crewMap[titleCrew.Tconst] = titleCrewMongo
	}

	fmt.Println("Parsing episodes")

	results, err = db.Query("SELECT episodeNumber, tconst, seasonNumber, parentTconst FROM title_episode")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var titleEpisode TitleEpisode

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titleEpisode.EpisodeNumber, &titleEpisode.Tconst, &titleEpisode.SeasonNumber, &titleEpisode.ParentTconst)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		episodeMap[titleEpisode.Tconst] = &titleEpisode
	}

	fmt.Println("Parsing akas")

	results, err = db.Query("SELECT types, isOriginalTitle, ordering, titleId, language, region, title, attributes FROM title_akas")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var titleAkas TitleAkas

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titleAkas.Types, &titleAkas.IsOriginalTitle, &titleAkas.Ordering, &titleAkas.Tconst, &titleAkas.Language, &titleAkas.Region, &titleAkas.Title, &titleAkas.Attributes)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		titleAkasMongo := new(TitleAkasMongo)
		titleAkasMongo.Types = titleAkas.Types
		titleAkasMongo.IsOriginalTitle = titleAkas.IsOriginalTitle
		titleAkasMongo.Ordering = titleAkas.Ordering
		titleAkasMongo.Language = titleAkas.Language
		titleAkasMongo.Region = titleAkas.Region
		titleAkasMongo.Title = titleAkas.Title
		titleAkasMongo.Attributes = titleAkas.Attributes

		if _, ok := akasMap[titleAkas.Tconst]; !ok {
			akasMap[titleAkas.Tconst] = make([]*TitleAkasMongo, 0)
		}

		akasMap[titleAkas.Tconst] = append(akasMap[titleAkas.Tconst], titleAkasMongo)
	}

	results, err = db.Query("SELECT runtimeMinutes, genres, endYear, tconst, primaryTitle, titleType, startYear, originalTitle, isAdult FROM title_basics")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	fmt.Println("Inserting title_basics")

	var offset = 250000
	var count = 0

	var titles []interface{}

	for results.Next() {

		var titleBasics TitleBasics

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titleBasics.RuntimeMinutes, &titleBasics.Genres, &titleBasics.EndYear, &titleBasics.Tconst, &titleBasics.PrimaryTitle, &titleBasics.TitleType, &titleBasics.StartYear, &titleBasics.OriginalTitle, &titleBasics.IsAdult)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		titleBasicsMongo := new(TitleBasicsMongo)
		titleBasicsMongo.Tconst = titleBasics.Tconst
		titleBasicsMongo.RuntimeMinutes = titleBasics.RuntimeMinutes
		titleBasicsMongo.EndYear = titleBasics.EndYear
		titleBasicsMongo.PrimaryTitle = titleBasics.PrimaryTitle
		titleBasicsMongo.TitleType = titleBasics.TitleType
		titleBasicsMongo.StartYear = titleBasics.StartYear
		titleBasicsMongo.OriginalTitle = titleBasics.OriginalTitle
		titleBasicsMongo.IsAdult = titleBasics.IsAdult

		if titleBasics.Genres.Valid {
			titleBasicsMongo.Genres = strings.Split(titleBasics.Genres.String, ",")
		} else {
			titleBasicsMongo.Genres = make([]string, 0)
		}

		if _, ok := crewMap[titleBasics.Tconst]; ok {
			titleBasicsMongo.Directors = crewMap[titleBasics.Tconst].Directors
			titleBasicsMongo.Writers = crewMap[titleBasics.Tconst].Writers

			delete(crewMap, titleBasics.Tconst)
		}

		if _, ok := ratingMap[titleBasics.Tconst]; ok {
			titleBasicsMongo.NumVotes = &ratingMap[titleBasics.Tconst].NumVote
			titleBasicsMongo.AverageRating = &ratingMap[titleBasics.Tconst].AverageRating

			delete(ratingMap, titleBasics.Tconst)
		}

		if _, ok := akasMap[titleBasics.Tconst]; ok {
			titleBasicsMongo.Akas = akasMap[titleBasics.Tconst]

			delete(akasMap, titleBasics.Tconst)
		} else {
			titleBasicsMongo.Akas = make([]*TitleAkasMongo, 0)
		}

		if _, ok := episodeMap[titleBasics.Tconst]; ok {
			titleBasicsMongo.EpisodeNumber = episodeMap[titleBasics.Tconst].EpisodeNumber
			titleBasicsMongo.SeasonNumber = episodeMap[titleBasics.Tconst].SeasonNumber
			titleBasicsMongo.ParentTconst = episodeMap[titleBasics.Tconst].ParentTconst

			delete(episodeMap, titleBasics.Tconst)
		}

		titles = append(titles, titleBasicsMongo)
		count++

		if count == offset {
			collection.InsertMany(context.TODO(), titles)

			count = 0
			titles = make([]interface{}, 0)
		}
	}

	if count > 0 {
		collection.InsertMany(context.TODO(), titles)
	}
}
