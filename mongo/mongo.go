// Common MongoDB code for Solve Microservices.

package mongo

import (
	"fmt"
	"os"
	"runtime"

	"github.com/globalsign/mgo"
)

// Create a new mongo connection
func WrapMongoConnection() MongoSession {
	var s *mgo.Session = NewMongoConnection()
	return &MgoSession{s}
}

func NewMongoConnection() *mgo.Session {
	fmt.Print("Initializing mongoDB connection \n")

	mgoUser := os.Getenv("DB_USER")
	mgoPassword := os.Getenv("DB_PASSWORD")
	mgoServer := os.Getenv("DB_SERVER")
	mgoPort := os.Getenv("DB_PORT")
	database := os.Getenv("DATABASE")

	var err error
	var dialInfo *mgo.DialInfo

	if runtime.GOOS == "darwin" {
		mgoServer = "127.0.0.1"
		mgoPort = "27017"
		mgoUser = ""
		mgoPassword = ""
		database = "solve"
		dialInfo, err = mgo.ParseURL(fmt.Sprintf("mongodb://%s:%s/%s", mgoServer, mgoPort, database))
	} else {
		if mgoServer == "" {
			fmt.Printf("$DB_SERVER must be set")
			os.Exit(-1)
		}
		if mgoPort == "" {
			fmt.Printf("Setting $DB_PORT to default value \n")
			mgoPort = "27017"
		}
		if database == "" {
			fmt.Printf("Setting $DATABASE to default value \n")
			database = "solve"
		}
		if mgoUser == "" {
			fmt.Printf("$DB_USER must be set")
			os.Exit(-1)
		}
		if mgoPassword == "" {
			fmt.Printf("$DB_PASSWORD must be set")
			os.Exit(-1)
		}
		dialInfo, err = mgo.ParseURL(fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", mgoUser, mgoPassword, mgoServer, mgoPort, database))
	}

	//tlsConfig := &tls.Config{}
	//tlsConfig.InsecureSkipVerify = true
	//
	//dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
	//	conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
	//	return conn, err
	//}
	//

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		fmt.Printf("Mongo connection error: %s\n", err)
		os.Exit(-1)
	}

	//session, err := mgo.Dial(mgoServer)
	//if err != nil {
	//	fmt.Printf("Mongo connection error: %s\n", err)
	//	os.Exit(-1)
	//}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	return session.Copy()
}

// Allow for mocking of MongoDB during tests.
type (
	// Must define local types to be allowed to define methods.
	MgoSession struct {
		Wrapped *mgo.Session
	}
	MgoDatabase struct {
		Wrapped *mgo.Database
	}
	MgoCollection struct {
		Wrapped *mgo.Collection
	}
	MgoQuery struct {
		Wrapped *mgo.Query
	}

	MongoSession interface {
		DB(name string) MongoDatabase
		Close()
	}

	MongoDatabase interface {
		C(name string) MongoCollection
	}

	MongoCollection interface {
		Find(query interface{}) MongoQuery
		Update(selector interface{}, update interface{}) error
		Insert(docs ...interface{}) error
	}

	MongoQuery interface {
		Select(selector interface{}) MongoQuery
		One(result interface{}) error
	}
)

func (s *MgoSession) DB(name string) MongoDatabase {
	return &MgoDatabase{s.Wrapped.DB(name)}
}

func (s *MgoSession) Close() {
	s.Wrapped.Close()
}

func (d *MgoDatabase) C(name string) MongoCollection {
	return &MgoCollection{d.Wrapped.C(name)}
}

func (c *MgoCollection) Update(selector interface{}, update interface{}) error {
	return c.Wrapped.Update(selector, update)
}

func (c *MgoCollection) Insert(docs ...interface{}) error {
	return c.Wrapped.Insert(docs)
}

func (c *MgoCollection) Find(query interface{}) MongoQuery {
	return &MgoQuery{c.Wrapped.Find(query)}
}

func (q *MgoQuery) Select(selector interface{}) MongoQuery {
	return &MgoQuery{q.Wrapped.Select(selector)}
}

func (q *MgoQuery) One(result interface{}) error {
	return q.Wrapped.One(result)
}
