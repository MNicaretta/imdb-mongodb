package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NameBasics struct {
	DeathYear         *int           `json:"deathYear"`
	BirthYear         *int           `json:"birthYear"`
	PrimaryName       *string        `json:"primaryName"`
	PrimaryProfession sql.NullString `json:"primaryProfession"`
	KnownForTitles    sql.NullString `json:"knownForTitles"`
	Nconst            int            `json:"nconst"`
}

type NameBasicsMongo struct {
	DeathYear         *int     `bson:"deathYear" json:"deathYear"`
	BirthYear         *int     `bson:"birthYear" json:"birthYear"`
	PrimaryName       *string  `bson:"primaryName" json:"primaryName"`
	PrimaryProfession []string `bson:"primaryProfession" json:"primaryProfession"`
	KnownForTitles    []string `bson:"knownForTitles" json:"knownForTitles"`
	Nconst            int      `bson:"nconst" json:"nconst"`
}

func main() {
	fmt.Println("Importing name_basics")

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/imdb2")

	// ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	collection := client.Database("imdb").Collection("name_basics")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()

	results, err := db.Query("SELECT deathYear, birthYear, primaryProfession, primaryName, knownForTitles, nconst FROM name_basics")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	var offset = 250000
	var count = 0

	var names []interface{}

	for results.Next() {

		var nameBasics NameBasics

		// for each row, scan the result into our tag composite object
		err = results.Scan(&nameBasics.DeathYear, &nameBasics.BirthYear, &nameBasics.PrimaryProfession, &nameBasics.PrimaryName, &nameBasics.KnownForTitles, &nameBasics.Nconst)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		nameBasicsMongo := new(NameBasicsMongo)
		nameBasicsMongo.DeathYear = nameBasics.DeathYear
		nameBasicsMongo.BirthYear = nameBasics.BirthYear
		nameBasicsMongo.PrimaryName = nameBasics.PrimaryName
		nameBasicsMongo.Nconst = nameBasics.Nconst

		if nameBasics.PrimaryProfession.Valid {
			nameBasicsMongo.PrimaryProfession = strings.Split(nameBasics.PrimaryProfession.String, ",")
		} else {
			nameBasicsMongo.PrimaryProfession = make([]string, 0)
		}

		if nameBasics.KnownForTitles.Valid {
			nameBasicsMongo.KnownForTitles = strings.Split(nameBasics.KnownForTitles.String, ",")
		} else {
			nameBasicsMongo.KnownForTitles = make([]string, 0)
		}

		names = append(names, nameBasicsMongo)
		count++

		if count == offset {
			collection.InsertMany(context.TODO(), names)

			count = 0
			names = make([]interface{}, 0)
		}
	}

	if count > 0 {
		collection.InsertMany(context.TODO(), names)
	}
}
