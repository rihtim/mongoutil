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
			Message: "Database connection failed. Reason: " + dialErr.Error(),
		}
		return
	}

	return
}

func (ma DataProvider) Create(collection string, data map[string]interface{}) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	connection := sessionCopy.DB(ma.Database).C(collection)

	createdAt := float64(time.Now().Unix())
	if _, hasId := data[ID]; !hasId {
		id := bson.NewObjectId()
		data[ID] = id.Hex()
	}
	data[CreatedAt] = createdAt
	data[UpdatedAt] = createdAt

	insertError := connection.Insert(data)
	if insertError != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Inserting item to database failed: " + insertError.Error(),
		}
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
	connection := sessionCopy.DB(ma.Database).C(collection)

	response = make(map[string]interface{})

	getErr := connection.FindId(id).One(&response)
	if getErr != nil {
		err = &utils.Error{
			Code:    http.StatusNotFound,
			Message: "Object from '" + collection + "' with id '" + id + "' not found.",
		}
		response = nil
		return
	}
	return
}

func (ma DataProvider) Query(collection string, parameters map[string][]string) (response map[string]interface{}, err *utils.Error) {

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()
	connection := sessionCopy.DB(ma.Database).C(collection)

	response = make(map[string]interface{})

	if parameters["aggregate"] != nil && parameters["where"] != nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Where and aggregate parameters cannot be used at the same request.",
		}
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
		getErr = connection.Pipe(aggregateParam).All(&results)
	} else {
		query := connection.Find(whereParam).Skip(skipParam).Limit(limitParam)
		if hasSortParam {
			query = query.Sort(sortParam)
		}
		getErr = query.All(&results)
	}

	if getErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Getting items failed. Reason: " + getErr.Error(),
		}
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
	connection := sessionCopy.DB(ma.Database).C(collection)

	if data == nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Request body cannot be empty for update requests.",
		}
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
			Message: "Update request to db failed.",
		}
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
	connection := sessionCopy.DB(ma.Database).C(collection)

	removeErr := connection.RemoveId(id)
	if removeErr != nil {
		err = &utils.Error{
			Code:    http.StatusNotFound,
			Message: "Deleting item failed. Reason: " + removeErr.Error(),
		}
	}
	return
}

func (ma DataProvider) CreateFile(data io.ReadCloser) (response map[string]interface{}, err *utils.Error) {

	if data == nil {
		err = &utils.Error{
			Code:    http.StatusBadRequest,
			Message: "Request body cannot be empty for create file requests.",
		}
		return
	}

	sessionCopy := ma.session.Copy()
	defer sessionCopy.Close()

	objectId := bson.NewObjectId()
	now := time.Now()
	fileName := objectId.Hex()

	gridFile, mongoErr := sessionCopy.DB(ma.Database).GridFS("fs").Create(fileName)
	if mongoErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Creating file failed. Reason: " + mongoErr.Error(),
		}
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
			Message: "Writing file failed. Reason: " + copyErr.Error(),
		}
		return
	}

	closeErr := gridFile.Close()
	if closeErr != nil {
		err = &utils.Error{
			Code:    http.StatusInternalServerError,
			Message: "Closing file failed. Reason: " + closeErr.Error(),
		}
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

	file, mongoErr := sessionCopy.DB(ma.Database).GridFS("fs").OpenId(id)
	if mongoErr != nil {
		if mongoErr == mgo.ErrNotFound {
			err = &utils.Error{
				Code:    http.StatusNotFound,
				Message: "File not found. Reason: " + mongoErr.Error(),
			}
		} else {
			err = &utils.Error{
				Code:    http.StatusInternalServerError,
				Message: "Getting file failed. Reason: " + mongoErr.Error(),
			}
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
