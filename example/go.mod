module github.com/kasorse/pgqueue/example

go 1.17

require (
	github.com/jmoiron/sqlx v1.3.4
	github.com/kasorse/pgqueue v0.0.0
)

replace github.com/kasorse/pgqueue v0.0.0 => ../

require (
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)
