package distributed

import (
	"context"
	"errors"
	"io"
	"time"

	devicev1 "github.com/antinvestor/apis/go/device/v1"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
	"golang.org/x/oauth2"
)

type devicesApi struct {
	serverName spec.ServerName

	jwtAudience string
	jwtIssuer   string
	svc         *frame.Service
	client      *devicev1.DeviceClient
}

func NewDevicesApi(
	ctx context.Context,
	deviceClient *devicev1.DeviceClient,
) (tables.DevicesTable, error) {

	svc := frame.Svc(ctx)

	cfg, ok := svc.Config().(*config.Global)
	if !ok {
		return nil, errors.New("failed to load global config")
	}

	return &devicesApi{
		client:      deviceClient,
		svc:         svc,
		serverName:  cfg.ServerName,
		jwtAudience: cfg.Oauth2JwtVerifyAudience,
		jwtIssuer:   cfg.Oauth2JwtVerifyIssuer,
	}, nil
}

func (d *devicesApi) toDeviceApi(localPart string, serverName spec.ServerName, device *devicev1.DeviceObject) *api.Device {

	dev := &api.Device{
		ID:          device.GetId(),
		SessionID:   device.GetSessionId(),
		DisplayName: device.GetName(),
		LastSeenIP:  device.GetIp(),
		UserAgent:   device.GetUserAgent(),
		Extra:       device.GetProperties().AsMap(),
	}

	lastSeen, err := time.Parse(time.RFC3339, device.GetLastSeen())
	if err == nil {
		dev.LastSeenTS = lastSeen.Unix()
	}

	if localPart != "" && serverName != "" {
		dev.UserID = userutil.MakeUserID(localPart, serverName)
	}

	return dev
}

func (d *devicesApi) InsertDevice(ctx context.Context, id, localpart string, serverName spec.ServerName, accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string) (*api.Device, error) {
	sessionID := util.IDString()
	return d.InsertDeviceWithSessionID(ctx, id, localpart, serverName, accessToken, extraData, displayName, ipAddr, userAgent, sessionID)
}

func (d *devicesApi) InsertDeviceWithSessionID(ctx context.Context, id, localpart string, serverName spec.ServerName, accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string, sessionID string) (*api.Device, error) {

	extras := frame.JSONMap{}
	if displayName != nil {
		extras["name"] = *displayName
	}

	req := devicev1.LogRequest{
		DeviceId:  id,
		SessionId: sessionID,
		Ip:        ipAddr,
		Locale:    "",
		UserAgent: userAgent,
		Os:        "",
		LastSeen:  time.Now().String(),
		Extras:    extras.ToProtoStruct(),
	}
	_, err := d.client.Svc().Log(ctx, &req)
	if err != nil {
		return nil, err
	}

	if accessToken != "" {
		_, dev, err0 := d.SelectDeviceByToken(ctx, accessToken)
		return dev, err0
	}

	return d.SelectDeviceByID(ctx, localpart, serverName, id)
}

func (d *devicesApi) DeleteDevice(ctx context.Context, id, _ string, _ spec.ServerName) error {
	req := devicev1.RemoveRequest{
		Id: id,
	}
	_, err := d.client.Svc().Remove(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}

func (d *devicesApi) DeleteDevices(ctx context.Context, localpart string, serverName spec.ServerName, devices []string) error {
	for _, devId := range devices {
		err := d.DeleteDevice(ctx, devId, localpart, serverName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *devicesApi) DeleteDevicesByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName, exceptDeviceID string) error {
	devices, err := d.SelectDevicesByLocalpart(ctx, localpart, serverName, exceptDeviceID)
	if err != nil {
		return err
	}

	var deviceIds []string
	for _, dev := range devices {
		deviceIds = append(deviceIds, dev.ID)
	}

	return d.DeleteDevices(ctx, localpart, serverName, deviceIds)
}

func (d *devicesApi) UpdateDeviceName(ctx context.Context, _ string, _ spec.ServerName, deviceID string, displayName *string) error {

	if displayName == nil {
		return nil
	}

	req := devicev1.UpdateRequest{
		Id:   deviceID,
		Name: *displayName,
	}
	_, err := d.client.Svc().Update(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}

func (d *devicesApi) SelectDeviceByToken(ctx context.Context, accessToken string) (context.Context, *api.Device, error) {

	ctx, err := d.svc.Authenticate(ctx, accessToken, d.jwtAudience, d.jwtIssuer)
	if err != nil {
		return ctx, nil, err
	}

	claims := frame.ClaimsFromContext(ctx)
	if claims == nil {
		return ctx, nil, errors.New("no claims found in authenticated context")
	}

	userIDStr := userutil.MakeUserID(claims.Subject, d.serverName)

	device := api.Device{
		ID:        claims.DeviceID,
		SessionID: claims.SessionID,
		UserID:    userIDStr,
	}

	device.Reload = func(ctx context.Context) error {
		userID, err0 := spec.NewUserID(device.UserID, false)
		if err0 != nil {
			return err0
		}

		rd, err0 := d.SelectDeviceByID(ctx, userID.Local(), d.serverName, device.ID)
		if err0 != nil {
			return err0
		}

		device.DisplayName = rd.DisplayName
		device.LastSeenTS = rd.LastSeenTS
		device.LastSeenIP = rd.LastSeenIP
		device.UserAgent = rd.UserAgent
		return nil
	}

	util.Log(ctx).
		WithField("access token", accessToken).
		WithField("device", device.ID).
		WithField("session id", device.SessionID).
		Debug("device found")

	return ctx, &device, nil

}

func (d *devicesApi) SelectDeviceByID(ctx context.Context, localpart string, serverName spec.ServerName, deviceID string) (*api.Device, error) {

	devices, err := d.SelectDevicesByID(ctx, []string{deviceID})
	if err != nil {
		return nil, err
	}

	if len(devices) == 0 {
		return nil, nil
	}

	dev := devices[0]
	dev.UserID = userutil.MakeUserID(localpart, serverName)

	return &dev, nil

}

func (d *devicesApi) SelectDevicesByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName, exceptDeviceID string) ([]api.Device, error) {

	req := devicev1.SearchRequest{
		Query: localpart,
	}
	stream, err := d.client.Svc().Search(ctx, &req)
	if err != nil {
		return nil, err
	}

	var devices []api.Device
	for {

		resp, err0 := stream.Recv()
		if err0 != nil {
			if err0 == io.EOF {
				return devices, nil
			}
			return devices, err0
		}

		for _, dev := range resp.GetData() {
			device := d.toDeviceApi(localpart, serverName, dev)
			devices = append(devices, *device)
		}
	}
}

func (d *devicesApi) SelectDevicesByID(ctx context.Context, deviceIDs []string) ([]api.Device, error) {
	req := devicev1.GetByIdRequest{
		Id: deviceIDs,
	}
	resp, err := d.client.Svc().GetById(ctx, &req)
	if err != nil {
		return nil, err
	}

	var devices []api.Device
	for _, dev := range resp.GetData() {
		device := d.toDeviceApi("", "", dev)
		devices = append(devices, *device)
	}

	return devices, nil
}

func (d *devicesApi) UpdateDeviceLastSeen(ctx context.Context, _ string, _ spec.ServerName, deviceID, ipAddr, userAgent string) error {

	claims := frame.ClaimsFromContext(ctx)
	if claims == nil {
		return errors.New("no claims found in authenticated context")
	}

	req := devicev1.LogRequest{
		DeviceId:  deviceID,
		SessionId: claims.SessionID,
		Ip:        ipAddr,
		Locale:    "",
		UserAgent: userAgent,
		Os:        "",
		LastSeen:  time.Now().String(),
	}
	_, err := d.client.Svc().Log(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}
