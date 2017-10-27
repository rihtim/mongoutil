package mongoutil

import (
	"net/http"
	"github.com/rihtim/core/utils"
	"github.com/rihtim/core/requestscope"
	"github.com/rihtim/core/messages"
	"github.com/rihtim/core/dataprovider"
)

// these fields are generated and maintained by
// mongoutil provider so having them in input are not allowed
var restrictedFields = []string{
	ID,
	CreatedAt,
	UpdatedAt,
}

// Checks body of the request. Returns error if the request body
// contains any restricted fields. Must be added to POST and PUT requests for all paths.
// Example Usage:
// core.Interceptors.Add(interceptors.AnyPath, methods.Post, interceptors.BEFORE_EXEC, mongoutil.ValidateInput, nil)
// core.Interceptors.Add(interceptors.AnyPath, methods.Put, interceptors.BEFORE_EXEC, mongoutil.ValidateInput, nil)
//
func ValidateInput(rs requestscope.RequestScope, extras interface{}, req, res messages.Message, db dataprovider.Provider) (editedReq, editedRes messages.Message, editedRs requestscope.RequestScope, err *utils.Error) {

	for _, field := range restrictedFields {
		if _, containsField := req.Body[field]; containsField {
			err = &utils.Error{
				Code:    http.StatusBadRequest,
				Message: "Input cannot contain '" + field + "' field.",
			}
			return
		}
	}
	return
}
