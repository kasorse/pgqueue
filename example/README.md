An example of using pgqueue.

Apply migrations:
```shell
$ goose -dir migrations -table goose_seed_version postgres "<dsn>" up
```

Start service:
```shell
$ PG_DSN='<dsn>' go run main.go
```

Schedule tasks:
```shell
$ curl -v -X POST 'http://localhost:8080/append_task?foobar=somesomesome'
```

will schedule the task with the payload `{"foobar": "somesomesome"}`.
