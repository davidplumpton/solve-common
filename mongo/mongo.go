// Common MongoDB code for Solve Microservices.

package mongo

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type (
	OffSetModel struct {
		ID       bson.ObjectId `bson:"_id,omitempty"`
		Position int64         `bson:"Position"`
	}

	CreateConnectionFunc func() MongoSession
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

// LocalOffSetRead reads local last offset position
func LocalOffSetRead(offSetFile string) (offSetPosition int64) {
	actualName := "."
	actualName += offSetFile

	_, err := os.Stat(actualName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Buffer file does not exist. Creating %s\n", actualName)
			newFile, err := os.OpenFile(
				actualName,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
				0666,
			)
			if err != nil {
				fmt.Printf("Cannot create file: %s \n", err)
				os.Exit(-1)
			}
			defer newFile.Close()

			fmt.Printf("New buffer file %s created. \n", actualName)

			// Write bytes to file
			byteSlice := []byte("-2")
			bytesWritten, err := newFile.Write(byteSlice)
			if err != nil {
				fmt.Printf("Cannot write to file: %s \n", err)
				os.Exit(-1)
			}
			fmt.Printf("Wrote %d bytes.\n", bytesWritten)

			offSetPosition = -2

			return offSetPosition
		}
	}
	fmt.Println("Opening local buffer file:")

	file, err := os.Open(actualName)
	if err != nil {
		fmt.Printf("Cannot open file: %s \n", err)
		os.Exit(-1)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Cannot read file: %s \n", err)
		os.Exit(-1)
	}

	fmt.Printf("Local buffer Position is: %s\n", data)

	intPosition, _ := strconv.ParseInt(string(data), 10, 64)
	intPosition = intPosition + 1
	offSetPosition = intPosition

	return offSetPosition
}

// MongoOffSetRead reads mongo last offset position
func MongoOffSetRead(createConnection CreateConnectionFunc, databaseName string, offSetCollection string) (offSetPosition int64, err error) {
	mongo := createConnection()
	collection := mongo.DB(databaseName).C(offSetCollection)

	fmt.Println("Preparing offset select statement")
	thisResult := OffSetModel{}
	offset := collection.Find(bson.M{}).One(&thisResult)

	if offset != nil {
		fmt.Println("No offset recorded for this topic, beggining one")
		collection.Insert(&OffSetModel{Position: -2})
		offSetPosition = -2
		err = nil
	} else {
		fmt.Println("Offset found in mongodb:")
		fmt.Println("MongoDB ObjectID: ", thisResult.ID)
		fmt.Println("MongoDB Offset Position: ", thisResult.Position)
		intPosition := thisResult.Position
		intPosition = intPosition + 1
		offSetPosition = intPosition
		err = nil
	}

	mongo.Close()
	return offSetPosition, err
}

// LocalOffSetWrite writes local last offset position
func LocalOffSetWrite(offSetFile string, offSetPosition int64) {
	actualName := "."
	actualName += offSetFile

	newFile, err := os.OpenFile(
		actualName,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Printf("Cannot create file: %s \n", err)
	}
	defer newFile.Close()

	//byteConvert := strconv.FormatInt(offSetPosition, 64)
	byteConvert := fmt.Sprintf("%v", offSetPosition)
	err = ioutil.WriteFile(actualName, []byte(byteConvert), 0666)
	if err != nil {
		fmt.Printf("Cannot write to file: %s \n", err)
	}

	fmt.Printf("Successfully wrote to file %v \n", actualName)

	return
}

// MongoOffSetWrite writes mongo last offset position
func MongoOffSetWrite(
	createConnection CreateConnectionFunc,
	databaseName string,
	offSetCollection string,
	offSetPosition int64) {
	//Store offset on mongo
	mongo := createConnection()
	collection := mongo.DB(databaseName).C(offSetCollection)

	fmt.Println("Preparing offset update statement")

	thisQuery := bson.M{}
	thisChange := bson.M{"$set": bson.M{"Position": offSetPosition}}

	fmt.Println("Executing offset update statement")

	err := collection.Update(thisQuery, thisChange)
	mongo.Close()

	if err != nil {
		fmt.Printf("Error Updating: %s %s", err, offSetCollection)
	} else {
		fmt.Println("Update completed: ", err)
	}

	fmt.Printf("Updated OffSet %v on %v in mongoDB \n", offSetPosition, offSetCollection)

	return
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
		Remove(selector interface{}) error
		Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	}

	MongoQuery interface {
		Select(selector interface{}) MongoQuery
		One(result interface{}) error
		Count() (int, error)
		All(result interface{}) error
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

func (c *MgoCollection) Remove(selector interface{}) error {
	return c.Wrapped.Remove(selector)
}

func (c *MgoCollection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	return c.Wrapped.Upsert(selector, update)
}

func (q *MgoQuery) Select(selector interface{}) MongoQuery {
	return &MgoQuery{q.Wrapped.Select(selector)}
}

func (q *MgoQuery) One(result interface{}) error {
	return q.Wrapped.One(result)
}

func (q *MgoQuery) Count() (int, error) {
	return q.Wrapped.Count()
}

func (q *MgoQuery) All(result interface{}) error {
	return q.Wrapped.All(result)
}
