package mongoutil

import (
	"io"
	"time"
	"reflect"
	"net/http"
	"encoding/json"
	"encoding/base64"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/rihtim/core/utils"
	"github.com/Sirupsen/logrus"
	"github.com/rihtim/core/log"
)

type DataProvider struct {
	Addresses    []string
	Database     string
	AuthDatabase string
	Username     string
	Password     string

	session  *mgo.Session
	dialInfo mgo.DialInfo
}

func (ma *DataProvider) Init() (err *utils.Error) {

	if ma.Addresses == nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Database 'addresses' must be specified.",
		}
		return
	}

	ma.dialInfo = mgo.DialInfo{
		Addrs:    ma.Addresses,
		Database: ma.AuthDatabase,
		Username: ma.Username,
		Password: ma.Password,
	}
	return
}

func (ma *DataProvider) Connect() (err *utils.Error) {

	var dialErr error
	ma.session, dialErr = mgo.DialWithInfo(&ma.dialInfo)
	if dialErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Database connection failed.",
		}

		log.WithFields(logrus.Fields{
			"reason": dialErr.Error(),
		}).Error("Mongo Error: Connection failed.")
		return
	}
	return
}

func (ma DataProvider) Create(collection string, data map[string]interface{}) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)
	connection := sessionCopy.DB(ma.Database).C(collection)

	createdAt := float64(time.Now().Unix())
	if _, hasId := data[ID]; !hasId {
		id := bson.NewObjectId()
		data[ID] = id.Hex()
	}
	data[CreatedAt] = createdAt
	data[UpdatedAt] = createdAt

	insertError := retry(5, func() (err error) {
		return connection.Insert(data)
	})

	if insertError != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: insertError.Error(),
		}

		log.WithFields(logrus.Fields{
			"reason":     insertError.Error(),
			"collection": collection,
			"data":       data,
		}).Error("Mongo Error: Inserting item failed.")
		return
	}

	response = map[string]interface{}{
		ID:        data[ID],
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
	return
}

func (ma DataProvider) Get(collection string, id string) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(300 * time.Millisecond)
	connection := sessionCopy.DB(ma.Database).C(collection)

	response = make(map[string]interface{})

	getErr := retry(5, func() (err error) {
		return connection.FindId(id).One(&response)
	})

	if getErr != nil {
		if getErr == mgo.ErrNotFound {
			err = &utils.Error{
				Code:    http.StatusNotFound,
				Message: "'" + collection + "' with id '" + id + "' not found.",
			}
		} else {
			err = &utils.Error{
				Code:    http.StatusInternalServerError,
				Message: "Getting '" + collection + "' with id '" + id + "' failed.",
			}
		}

		response = nil
		log.WithFields(logrus.Fields{
			"reason":     getErr.Error(),
			"collection": collection,
			"id":         id,
		}).Error("Mongo Error: Getting item failed.")
		return
	}
	return
}

func (ma DataProvider) Query(collection string, parameters map[string][]string) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)
	connection := sessionCopy.DB(ma.Database).C(collection)

	response = make(map[string]interface{})

	if parameters["aggregate"] != nil && parameters["where"] != nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Where and aggregate parameters cannot be used at the same request.",
		}

		log.Error("Mongo Error: Where and aggregate parameters cannot be used at the same request.")
		return
	}

	var results []map[string]interface{}
	var getErr error

	whereParam, hasWhereParam, whereParamErr := extractJsonParameter(parameters, "where")
	aggregateParam, hasAggregateParam, aggregateParamErr := extractJsonParameter(parameters, "aggregate")
	sortParam, hasSortParam, sortParamErr := extractStringParameter(parameters, "sort")
	limitParam, _, limitParamErr := extractIntParameter(parameters, "limit")
	skipParam, _, skipParamErr := extractIntParameter(parameters, "skip")

	if aggregateParamErr != nil {
		err = aggregateParamErr
	}
	if whereParamErr != nil {
		err = whereParamErr
	}
	if sortParamErr != nil {
		err = sortParamErr
	}
	if limitParamErr != nil {
		err = limitParamErr
	}
	if skipParamErr != nil {
		err = skipParamErr
	}
	if err != nil {
		return
	}

	if hasWhereParam && hasAggregateParam {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Aggregation cannot be used with where parameter.",
		}
		return
	}

	if hasAggregateParam {
		getErr = retry(5, func() (err error) {
			return connection.Pipe(aggregateParam).All(&results)
		})
	} else {
		query := connection.Find(whereParam).Skip(skipParam).Limit(limitParam)
		if hasSortParam {
			query = query.Sort(sortParam)
		}
		getErr = retry(5, func() (err error) {
			return query.All(&results)
		})
	}

	if getErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Querying items from database failed. Reason: " + getErr.Error(),
		}

		log.WithFields(logrus.Fields{
			"reason":     getErr.Error(),
			"collection": collection,
			"parameters": parameters,
		}).Error("Mongo Error: Querying items failed.")
		return
	}

	if results != nil {
		response["results"] = results
	} else {
		response["results"] = make([]map[string]interface{}, 0)
	}
	return
}

func (ma DataProvider) Update(collection string, id string, data map[string]interface{}) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)
	connection := sessionCopy.DB(ma.Database).C(collection)

	if data == nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Request body cannot be empty for update requests.",
		}

		log.Error("Mongo Error: Request body cannot be empty for update requests.")
		return
	}

	data[UpdatedAt] = int32(time.Now().Unix())

	objectToUpdate := make(map[string]interface{})
	findErr := connection.FindId(id).One(&objectToUpdate)
	if findErr != nil {
		err = &utils.Error{
			Code:    http.StatusNotFound,
			Message: "Item not found.",
		}
		return
	}

	// updating the fields that request body contains
	for k, v := range data {
		objectToUpdate[k] = v
	}

	updateErr := connection.UpdateId(id, objectToUpdate)
	if updateErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Updating '" + collection + "' with id '" + id + "' failed.",
		}

		log.WithFields(logrus.Fields{
			"reason":     updateErr.Error(),
			"collection": collection,
			"id":         id,
		}).Error("Mongo Error: Updating item failed.")
		return
	}

	response = map[string]interface{}{
		UpdatedAt: data[UpdatedAt],
	}
	return
}

func (ma DataProvider) Delete(collection string, id string) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)
	connection := sessionCopy.DB(ma.Database).C(collection)

	removeErr := connection.RemoveId(id)
	if removeErr != nil {
		err = &utils.Error{
			Code:    http.StatusNotFound,
			Message: "Updating '" + collection + "' with id '" + id + "' failed.",
		}

		log.WithFields(logrus.Fields{
			"reason":     removeErr.Error(),
			"collection": collection,
			"id":         id,
		}).Error("Mongo Error: Updating item failed.")
	}
	return
}

func (ma DataProvider) CreateFile(data io.ReadCloser) (response map[string]interface{}, err *utils.Error) {

	if data == nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Request body cannot be empty for create file requests.",
		}

		log.Error("Mongo Error: Request body cannot be empty for create file requests.")
		return
	}

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)

	objectId := bson.NewObjectId()
	now := time.Now()
	fileName := objectId.Hex()

	gridFile, mongoErr := sessionCopy.DB(ma.Database).GridFS("fs").Create(fileName)
	if mongoErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Creating file failed.",
		}

		log.WithFields(logrus.Fields{
			"reason": mongoErr.Error(),
		}).Error("Creating file failed.")
		return
	}
	gridFile.SetId(fileName)
	gridFile.SetName(fileName)
	gridFile.SetUploadDate(now)

	dec := base64.NewDecoder(base64.StdEncoding, data)
	_, copyErr := io.Copy(gridFile, dec)
	if copyErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Writing file failed.",
		}

		log.WithFields(logrus.Fields{
			"reason": copyErr.Error(),
		}).Error("Mongo Error: Writing file failed.")
		return
	}

	closeErr := gridFile.Close()
	if closeErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Closing file failed.",
		}

		log.WithFields(logrus.Fields{
			"reason": closeErr.Error(),
		}).Error("Mongo Error: Closing file failed.")
		return
	}

	response = map[string]interface{}{
		ID:        fileName,
		CreatedAt: int32(now.Unix()),
	}
	return
}

func (ma DataProvider) GetFile(id string) (response []byte, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	sessionCopy.SetSyncTimeout(1 * time.Second)
	sessionCopy.SetSocketTimeout(1 * time.Second)

	file, mongoErr := sessionCopy.DB(ma.Database).GridFS("fs").OpenId(id)
	if mongoErr != nil {
		if mongoErr == mgo.ErrNotFound {
			err = &utils.Error{
				Code:    http.StatusNotFound,
				Message: "File not found.",
			}

			log.WithFields(logrus.Fields{
				"reason": mongoErr.Error(),
			}).Error("Mongo Error: File not found.")
		} else {
			err = &utils.Error{
				Code:    http.StatusInternalServerError,
				Message: "Getting file failed.",
			}

			log.WithFields(logrus.Fields{
				"reason": mongoErr.Error(),
			}).Error("Mongo Error: Getting file failed.")
		}
		return
	}

	response = make([]byte, file.Size())
	_, printErr := file.Read(response)
	if printErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Printing file failed. Reason: " + printErr.Error(),
		}

		log.WithFields(logrus.Fields{
			"reason": printErr.Error(),
		}).Error("Mongo Error: Printing file failed.")
	}
	file.Close()
	return
}

var extractJsonParameter = func(parameters map[string][]string, key string) (value interface{}, hasParam bool, err *utils.Error) {

	var paramArray []string
	paramArray, hasParam = parameters[key]

	if hasParam {
		parseErr := json.Unmarshal([]byte(paramArray[0]), &value)
		if parseErr != nil {
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "Parsing " + key + " parameter failed. Reason: " + parseErr.Error(),
			}
		}
	}
	return
}

var extractStringParameter = func(parameters map[string][]string, key string) (value string, hasParam bool, err *utils.Error) {

	var paramArray []string
	paramArray, hasParam = parameters[key]

	if hasParam {
		var paramValue interface{}
		parseErr := json.Unmarshal([]byte(paramArray[0]), &paramValue)
		if parseErr != nil {
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "Parsing " + key + " parameter failed. Reason: " + parseErr.Error(),
			}
		}

		fieldType := reflect.TypeOf(paramValue)
		if fieldType == nil || fieldType.Kind() != reflect.String {
			value = ""
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "The key '" + key + "' must be a valid string.",
			}
			return
		}
		value = paramValue.(string)
	}
	return
}

var extractIntParameter = func(parameters map[string][]string, key string) (value int, hasParam bool, err *utils.Error) {

	var paramArray []string
	paramArray, hasParam = parameters[key]

	if hasParam {
		var paramValue interface{}
		parseErr := json.Unmarshal([]byte(paramArray[0]), &paramValue)
		if parseErr != nil {
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "Parsing " + key + " parameter failed. Reason: " + parseErr.Error(),
			}
		}

		fieldType := reflect.TypeOf(paramValue)
		if fieldType == nil || fieldType.Kind() != reflect.Float64 {
			value = 0
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "The key '" + key + "' must be an integer.",
			}
			return
		}
		value = int(paramValue.(float64))
	}
	return
}

func retry(attempts int, function func() error) (err error) {
	for i := 0; ; i++ {
		err = function()

		// finish if the function suceeded
		if err == nil {
			return
		}

		// no need to retry if the error is 'not found' error
		if err == mgo.ErrNotFound {
			return
		}

		// break if the last attempt failed too
		if i >= (attempts - 1) {
			break
		}

		log.WithFields(logrus.Fields{
			"reason":  err.Error(),
			"attempt": i + 1,
		}).Error("Mongo Error: Attempt failed. Retrying.")
	}

	log.WithFields(logrus.Fields{
		"reason":  err.Error(),
		"attempt": attempts,
	}).Error("Mongo Error: Last attempt failed. Not retrying.")

	return err
}
