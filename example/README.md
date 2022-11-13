Пример использования pgqueue.

Применить миграции:
```shell
$ goose -dir migrations -table goose_seed_version postgres "<dsn>" up
```

Запустить сервис:
```shell
$ PG_DSN='<dsn>' go run main.go
```

Планировать задачи:
```shell
$ curl -v -X POST 'http://localhost:8080/append_task?foobar=somesomesome'
```

запланирует задачу с пейлоадом `{"foobar": "somesomesome"}`. 
