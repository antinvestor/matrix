package routing

import (
	"net/http"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type newPasswordRequest struct {
	NewPassword   string          `json:"new_password"`
	LogoutDevices bool            `json:"logout_devices"`
	Auth          newPasswordAuth `json:"auth"`
}

type newPasswordAuth struct {
	Type    string `json:"type"`
	Session string `json:"session"`
	auth.PasswordRequest
}

func Password(
	req *http.Request,
	userAPI api.ClientUserAPI,
	device *api.Device,
	cfg *config.ClientAPI,
) util.JSONResponse {
	// Check that the existing password is right.
	var r newPasswordRequest
	r.LogoutDevices = true

	util.Log(req.Context()).
		WithField("sessionId", device.SessionID).
		WithField("userId", device.UserID).
		Debug("Changing password")

	// Unmarshal the request.
	resErr := httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	// Retrieve or generate the sessionID
	sessionID := r.Auth.Session
	if sessionID == "" {
		// Generate a new, random session ID
		sessionID = util.RandomString(sessionIDLength)
	}

	// Require password auth to change the password.
	if r.Auth.Type != authtypes.LoginTypePassword {
		return util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: newUserInteractiveResponse(
				sessionID,
				[]authtypes.Flow{
					{
						Stages: []authtypes.LoginType{authtypes.LoginTypePassword},
					},
				},
				nil,
			),
		}
	}

	// Check if the existing password is correct.
	typePassword := auth.LoginTypePassword{
		GetAccountByPassword: userAPI.QueryAccountByPassword,
		Config:               cfg,
	}
	if _, authErr := typePassword.Login(req.Context(), &r.Auth.PasswordRequest); authErr != nil {
		return *authErr
	}
	sessions.addCompletedSessionStage(sessionID, authtypes.LoginTypePassword)

	// Check the new password strength.
	if err := internal.ValidatePassword(r.NewPassword); err != nil {
		return *internal.PasswordResponse(err)
	}

	// Get the local part.
	localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Ask the user API to perform the password change.
	passwordReq := &api.PerformPasswordUpdateRequest{
		Localpart:  localpart,
		ServerName: domain,
		Password:   r.NewPassword,
	}
	passwordRes := &api.PerformPasswordUpdateResponse{}
	if err := userAPI.PerformPasswordUpdate(req.Context(), passwordReq, passwordRes); err != nil {
		util.Log(req.Context()).WithError(err).Error("PerformPasswordUpdate failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !passwordRes.PasswordUpdated {
		util.Log(req.Context()).Error("Expected password to have been updated but wasn't")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// If the request asks us to log out all other devices then
	// ask the user API to do that.
	if r.LogoutDevices {
		logoutReq := &api.PerformDeviceDeletionRequest{
			UserID:         device.UserID,
			DeviceIDs:      nil,
			ExceptDeviceID: device.ID,
		}
		logoutRes := &api.PerformDeviceDeletionResponse{}
		if err := userAPI.PerformDeviceDeletion(req.Context(), logoutReq, logoutRes); err != nil {
			util.Log(req.Context()).WithError(err).Error("PerformDeviceDeletion failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		pushersReq := &api.PerformPusherDeletionRequest{
			Localpart:  localpart,
			ServerName: domain,
			SessionID:  device.SessionID,
		}
		if err := userAPI.PerformPusherDeletion(req.Context(), pushersReq, &struct{}{}); err != nil {
			util.Log(req.Context()).WithError(err).Error("PerformPusherDeletion failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// Return a success code.
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
