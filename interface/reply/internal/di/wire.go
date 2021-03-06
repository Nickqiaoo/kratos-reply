// +build wireinject
// The build tag makes sure the stub is not built in the final build.

package di

import (
	"kratos-reply/internal/dao"
	"kratos-reply/internal/service"
	"kratos-reply/internal/server/http"

	"github.com/google/wire"
)

//go:generate kratos t wire
func InitApp() (*App, func(), error) {
	panic(wire.Build(dao.Provider, service.New, http.New, NewApp))
}