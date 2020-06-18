package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TitlePrincipal struct {
	Job        *string        `json:"job"`
	Ordering   *int           `json:"ordering"`
	Category   *string        `json:"category"`
	Characters sql.NullString `json:"characters"`
	Tconst     int            `json:"tconst"`
	Nconst     int            `json:"nconst"`
}

type TitlePrincipalMongo struct {
	Job        *string  `bson:"job" json:"job"`
	Ordering   *int     `bson:"ordering" json:"ordering"`
	Category   *string  `bson:"category" json:"category"`
	Characters []string `bson:"characters" json:"characters"`
	Tconst     int      `bson:"tconst" json:"tconst"`
	Nconst     int      `bson:"nconst" json:"nconst"`
}

func main() {
	fmt.Println("Importing title_principals")

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/imdb2")

	// ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	collection := client.Database("imdb").Collection("title_principals")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()

	results, err := db.Query("SELECT job, ordering, category, characters, tconst, nconst FROM title_principals")

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	var offset = 1000000
	var count = 0

	var principals []interface{}

	for results.Next() {

		var titlePrincipal TitlePrincipal

		// for each row, scan the result into our tag composite object
		err = results.Scan(&titlePrincipal.Job, &titlePrincipal.Ordering, &titlePrincipal.Category, &titlePrincipal.Characters, &titlePrincipal.Tconst, &titlePrincipal.Nconst)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		titlePrincipalMongo := new(TitlePrincipalMongo)
		titlePrincipalMongo.Job = titlePrincipal.Job
		titlePrincipalMongo.Ordering = titlePrincipal.Ordering
		titlePrincipalMongo.Category = titlePrincipal.Category
		titlePrincipalMongo.Tconst = titlePrincipal.Tconst
		titlePrincipalMongo.Nconst = titlePrincipal.Nconst
		titlePrincipalMongo.Characters = make([]string, 0)

		principals = append(principals, titlePrincipalMongo)
		count++

		if count == offset {
			collection.InsertMany(context.TODO(), principals)

			count = 0
			principals = make([]interface{}, 0)
		}
	}

	if count > 0 {
		collection.InsertMany(context.TODO(), principals)
	}
}
