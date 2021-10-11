package store

type RedisConfig struct {
	Addr	  	string
	Password  	string
	DB			int
}

func SetDefault(redis RedisConfig)  {
	_redisConfig = redis
	if err := Redis().Open(); err != nil {
		panic(err)
	}
}
