package middleware

type Middleware struct {
	middlewares map[string]interface{}
}

func NewMiddleware(middlewares map[string]interface{}) *Middleware {
	return &Middleware{
		middlewares: middlewares,
	}
}
